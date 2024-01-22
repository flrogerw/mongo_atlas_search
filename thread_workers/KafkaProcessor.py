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
from pubsub.KafkaFetcher import KafkaFetcher
import traceback
from dotenv import load_dotenv
from lxml import etree


load_dotenv()
# Load System ENV VARS
UPLOAD_BUCKET = os.getenv('UPLOAD_BUCKET')
UUID_NAMESPACE = os.getenv('UUID_NAMESPACE')
REQUIRED_ELEMENTS = os.getenv('REQUIRED_ELEMENTS').split(",")
LANGUAGES = os.getenv('LANGUAGES').split(",")
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_USER = os.getenv('REDIS_USER')
REDIS_PASS = os.getenv('REDIS_PASS')
MIN_DESCRIPTION_LENGTH = int(os.getenv('MIN_DESCRIPTION_LENGTH'))
MIN_TITLE_LENGTH = int(os.getenv('MIN_TITLE_LENGTH'))


class KafkaProcessor(threading.Thread):
    def __init__(self, bad_queue, quarantine_queue, purgatory_queue, thread_lock, topic, *args, **kwargs):
        self.thread_lock = thread_lock
        self.error_queue = bad_queue
        self.quarantine_queue = quarantine_queue
        self.purgatory_queue = purgatory_queue
        self.s3 = boto3.client("s3")
        self.redis = redis.Redis(host=REDIS_HOST, port=6379, charset="utf-8", decode_responses=True)
        self.namespace = uuid.uuid5(uuid.NAMESPACE_DNS, UUID_NAMESPACE)
        self.kafka_topic = topic
        super().__init__(*args, **kwargs)

    def run(self):
        kafka = KafkaFetcher(self.kafka_topic)
        while True:
            try:
                task = kafka.fetch_one()
                print(task)
                # task = self.job_queue.get()
                #self.pre_process(task)
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
            self.error_queue.put({"file_name": file_name,
                                  "error": error_str,
                                  "stack_trace": stack_trace.replace("\x00", "\uFFFD")})

    def log_to_purgatory(self, kafka_message, xml, error):
        # print(error)
        try:
            title = 'n/a'
            description = 'n/a'
            author = 'n/a'

            xml_title = xml.find(".//channel/title")
            xml_description = xml.find(".//channel/description")
            xml_author = xml.find(".//channel/author")

            if hasattr(xml_title, 'text') and xml_title.text:
                title = ProcessText.return_clean_text(xml_title.text)
            if hasattr(xml_description, 'text') and xml_description.text:
                description = ProcessText.return_clean_text(xml_description.text)
            if hasattr(xml_author, 'text') and xml_author.text:
                author = ProcessText.return_clean_text(xml_author.text)

            log_entry = {
                "file_name": kafka_message['file_name'],
                "file_hash": kafka_message['file_hash'],
                "podcast_uuid": kafka_message['podcast_uuid'],
                "language": kafka_message['language'],
                "reason_for_failure": error,
                "title_cleaned": title,
                "description_cleaned": description,
                "author": author,
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

    @staticmethod
    def validate_text_length(response):
        try:
            description_len = len(response['description_cleaned'].split(' '))
            title_len = len(response['title_cleaned'].split(' '))

            if description_len < MIN_DESCRIPTION_LENGTH or title_len < MIN_TITLE_LENGTH:
                raise ValidationError(
                    "Minimum length(s) not met: title {}:{}, description {}:{}."
                    .format(title_len, MIN_TITLE_LENGTH, description_len,
                            MIN_DESCRIPTION_LENGTH))
        except Exception:
            raise
    def pre_process(self, task):
        try:
            xml = self.get_from_web(task['feedFilePath'])
            root = etree.XML(xml)
            kafka_message = {'podcast_uuid': str(uuid.uuid5(self.namespace, task['feedFilePath'])),
                             'file_hash': hashlib.md5(str(xml).encode()).hexdigest()}
            kafka_message['file_name'] = kafka_message['file_hash'] + '.rss.xml'
            kafka_message['language'] = None
            root = etree.XML(xml)
            return
            previous_podcast_uuid = self.redis.get(kafka_message['file_hash'])
            # Check for Exact Duplicates using hash of entire file string and hash of URL.
            if previous_podcast_uuid == kafka_message['podcast_uuid']:
                raise ValidationError(
                    "File {} is a duplicate to: {}.".format(task['feedFilePath'], previous_podcast_uuid))
            # Same Body Different URL. title says "DELETED"??
            elif previous_podcast_uuid:
                raise QuarantineError(previous_podcast_uuid)
            # Set entry in Redis
            else:
                self.redis.set(kafka_message['file_hash'], kafka_message['podcast_uuid'])

            # Set some basic values
            # kafka_message['episode_count'] = len(list(root.iter("item")))
            #self.post_to_s3(xml, kafka_message['file_name'])
            #print(kafka_message)

        except ClientError as err:
            self.log_to_errors(task['feedFilePath'], str(err), traceback.format_exc())

        except ValidationError as err:
            self.log_to_purgatory(kafka_message, root, str(err))

        except QuarantineError as previous_podcast_uuid:
            self.log_to_quarantine(kafka_message['podcast_uuid'], str(previous_podcast_uuid), task['feedFilePath'])

        except Exception as err:
            self.log_to_errors(task['feedFilePath'], str(err), traceback.format_exc())


