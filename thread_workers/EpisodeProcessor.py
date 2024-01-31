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
READABILITY_FIELD = os.getenv('FIELD_FOR_READABILITY')
GET_TOKENS = os.getenv('GET_TOKENS').split(",")
FIELD_TO_VECTOR = os.getenv('PODCAST_VECTOR')
ELEMENTS_TO_PROCESS = os.getenv('ELEMENTS_TO_PROCESS').split(",")
MIN_LANGUAGE_TOLERANCE = os.getenv('MIN_LANGUAGE_TOLERANCE')


class KafkaEpisodeProcessor(threading.Thread):
    def __init__(self, jobs_q, quality_q, errors_q, thread_lock, nlp, model, *args,
                 **kwargs):
        self.nlp = nlp
        self.thread_lock = thread_lock
        self.model = model
        self.job_queue = jobs_q
        self.quality_q = quality_q
        self.thread_lock = thread_lock
        self.error_queue = errors_q
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

    def log_to_errors(self, file_name, error_str, stack_trace):
        # print(error_str, stack_trace)
        with self.thread_lock:
            self.error_queue.put({"identifier": file_name,
                                  "entity_type": 'podcast',
                                  "error": error_str,
                                  "stack_trace": stack_trace.replace("\x00", "\uFFFD")})

    @staticmethod
    def get_from_web(file_path):
        try:
            rss = requests.get(file_path, timeout=2.0)
            return rss.text.encode()
        except Exception:
            raise

    def get_nlp_fields(self, kafka_message):
        for key in kafka_message.keys():
            if key in GET_TOKENS:
                lemma_key = f"{key.split('_')[0]}_lemma"
                kafka_message[lemma_key] = ProcessText.return_lemma(kafka_message[key], kafka_message['language'])
            kafka_message['vector'] = pickle.dumps(ProcessText.get_vector(kafka_message[FIELD_TO_VECTOR], self.model))

    def pre_process(self, kafka_message):
        try:
            # Creates Search fields
            self.get_nlp_fields(kafka_message)

            # Extra Processing goes Here
            if kafka_message['language'] == 'en':
                kafka_message['readability'] = ProcessText.get_readability(kafka_message[READABILITY_FIELD], self.nlp)

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
