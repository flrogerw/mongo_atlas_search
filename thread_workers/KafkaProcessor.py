from Errors import QuarantineError, ValidationError
import boto3
from botocore.exceptions import ClientError
import os
import threading
import queue
import uuid
import redis
import hashlib
import requests
from nlp.ProcessText import ProcessText
import traceback
from dotenv import load_dotenv
from lxml import etree
import pickle

load_dotenv()
# Load System ENV VARS
UPLOAD_BUCKET = os.getenv('UPLOAD_BUCKET')
UUID_NAMESPACE = os.getenv('UUID_NAMESPACE')
LANGUAGES = os.getenv('LANGUAGES').split(",")
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_USER = os.getenv('REDIS_USER')
REDIS_PASS = os.getenv('REDIS_PASS')
MIN_DESCRIPTION_LENGTH = int(os.getenv('MIN_PODCAST_DESC_LENGTH'))
MIN_TITLE_LENGTH = int(os.getenv('MIN_PODCAST_TITLE_LENGTH'))
FIELD_FOR_READABILITY = os.getenv('FIELD_FOR_READABILITY')
GET_TOKENS = os.getenv('GET_TOKENS').split(",")
FIELD_TO_VECTOR = os.getenv('PODCAST_VECTOR')
ELEMENTS_TO_PROCESS = os.getenv('ELEMENTS_TO_PROCESS').split(",")
MIN_LANGUAGE_TOLERANCE = os.getenv('MIN_LANGUAGE_TOLERANCE')


class KafkaProcessor(threading.Thread):
    def __init__(self, jobs_q, quality_q, errors_q, quarantine_q, purgatory_q, thread_lock, nlp, model, *args,
                 **kwargs):
        self.nlp = nlp
        self.thread_lock = thread_lock
        self.model = model
        self.job_queue = jobs_q
        self.quality_q = quality_q
        self.thread_lock = thread_lock
        self.error_queue = errors_q
        self.quarantine_queue = quarantine_q
        self.purgatory_queue = purgatory_q
        self.s3 = boto3.client("s3")
        self.redis = redis.Redis(host=REDIS_HOST, port=6379, charset="utf-8", decode_responses=True)
        self.namespace = uuid.uuid5(uuid.NAMESPACE_DNS, UUID_NAMESPACE)
        super().__init__(*args, **kwargs)

    def run(self):
        while True:
            try:
                task = self.job_queue.get(timeout=5)
                self.pre_process(task)
            except queue.Empty:
                return

    def log_to_quarantine(self, podcast_uuid, matching_uuid, file_name):
        # print(podcast_uuid, matching_uuid, file_name)
        with self.thread_lock:
            self.quarantine_queue.put({"podcast_uuid": podcast_uuid,
                                       "original_podcast_uuid": matching_uuid,
                                       "duplicate_file_name": file_name})

    def log_to_errors(self, file_name, error_str, stack_trace):
        # print(error_str, stack_trace)
        with self.thread_lock:
            self.error_queue.put({"identifier": file_name,
                                  "entity_type": 'podcast',
                                  "error": error_str,
                                  "stack_trace": stack_trace.replace("\x00", "\uFFFD")})

    def log_to_purgatory(self, kafka_message, error):
        # print(error)
        try:
            log_entry = {
                "file_name": kafka_message['file_name'],
                "file_hash": kafka_message['file_hash'],
                "podcast_uuid": kafka_message['podcast_uuid'],
                "language": kafka_message['language'],
                "reason_for_failure": error,
                "title_cleaned": kafka_message['title_cleaned']
                if hasattr(kafka_message, 'title_cleaned') else ProcessText.return_clean_text(kafka_message['title']),
                "description_cleaned": kafka_message['description_cleaned']
                if hasattr(kafka_message, 'description_cleaned') else ProcessText.return_clean_text(kafka_message['description']),
                "author": kafka_message['author'] if kafka_message['author'] else 'n/a',
                "index_status": 320
            }
            with self.thread_lock:
                self.purgatory_queue.put(log_entry)
        except Exception:
            raise

    @staticmethod
    def get_from_web(file_path):
        try:
            rss = requests.get(file_path, timeout=2.0)
            return rss.text.encode()
        except Exception:
            raise

    def post_to_s3(self, xml, file_name):
        self.s3.put_object(Body=str(xml), Bucket=UPLOAD_BUCKET, Key=file_name)


    @staticmethod
    def get_nlp_fields(kafka_message):
        for e in ELEMENTS_TO_PROCESS:
            clean_text = ProcessText(kafka_message[e], kafka_message['language'])
            del kafka_message[e]
            kafka_message[e + "_cleaned"] = clean_text.get_clean()
            if e in GET_TOKENS:
                kafka_message[e + '_lemma'] = clean_text.get_tokens()

    def pre_process(self, kafka_message):
        try:
            # xml = self.get_from_web(kafka_message['rss_url'])
            # root = etree.XML(xml)
            # root = etree.XML(xml)
            # self.process_episodes(root, kafka_message['podcast_uuid'])
            job_hash = hashlib.md5(str(kafka_message).encode()).hexdigest()
            kafka_message.update({'file_hash': job_hash, 'file_name': f'{job_hash}.rss.xml'})

            # Check for instance in Redis
            previous_podcast_uuid = self.redis.get(kafka_message['file_hash'])
            # Check for Exact Duplicates using hash of entire file string and hash of URL.
            if previous_podcast_uuid == kafka_message['podcast_uuid']:
                raise ValidationError(
                    "File {} is a duplicate to: {}.".format(kafka_message['rss_url'], previous_podcast_uuid))

            # Same Body Different URL. title says "DELETED"??
            elif previous_podcast_uuid:
                raise QuarantineError(previous_podcast_uuid)
            # Set entry in Redis
            else:
                self.redis.set(kafka_message['file_hash'], kafka_message['podcast_uuid'])

            self.get_nlp_fields(kafka_message)
            kafka_message['vector'] = pickle.dumps(ProcessText.get_vector(kafka_message[FIELD_TO_VECTOR], self.model))
            # Limited by Language Functions
            if kafka_message['language'] == 'en':
                kafka_message['readability'] = ProcessText.get_readability(kafka_message[FIELD_FOR_READABILITY],
                                                                           self.nlp)

            # Post kafka_message in the completed queue and S3.
            # self.s3.put_object(Body=str(xml), Bucket=UPLOAD_BUCKET, Key=kafka_message['file_name'])
            with self.thread_lock:
                self.quality_q.put(kafka_message)

        except ClientError as err:
            self.log_to_errors(kafka_message['rss_url'], str(err), traceback.format_exc())

        except ValidationError as err:
            self.log_to_purgatory(kafka_message, str(err))

        except QuarantineError as previous_podcast_uuid:
            self.log_to_quarantine(kafka_message['podcast_uuid'], str(previous_podcast_uuid), kafka_message['rss_url'])

        except Exception as err:
            self.log_to_errors(kafka_message['rss_url'], str(err), traceback.format_exc())
