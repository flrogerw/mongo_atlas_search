import os
import uuid
import queue
import hashlib
import traceback
import requests
import redis
import threading
from confluent_kafka import KafkaException
# from confluent_avro import AvroKeyValueSerde, SchemaRegistry
# from confluent_avro.schema_registry import HTTPBasicAuth
from dotenv import load_dotenv
from Errors import QuarantineError
from logger.Logger import ErrorLogger
from schemas.validation import Podcast
from urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

load_dotenv()
LANGUAGES = os.getenv('LANGUAGES').split(",")
REQUIRED_ELEMENTS = os.getenv('REQUIRED_ELEMENTS').split(",")
UUID_NAMESPACE = os.getenv('UUID_NAMESPACE')
MIN_DESCRIPTION_LENGTH = int(os.getenv('MIN_PODCAST_DESC_LENGTH'))
MIN_TITLE_LENGTH = int(os.getenv('MIN_PODCAST_TITLE_LENGTH'))
REDIS_HOST = os.getenv('REDIS_HOST')
#IMAGE_MIME_TYPES = os.getenv('IMAGE_MIME_TYPES').split(",")
#KAFKA_UPLOAD_TOPIC = os.getenv('KAFKA_UPLOAD_TOPIC')
#DEFAULT_IMAGE_PATH = os.getenv('DEFAULT_IMAGE_PATH')
#UPLOAD_BUCKET = os.getenv('UPLOAD_BUCKET')
#READABILITY_FIELD = os.getenv('FIELD_FOR_READABILITY')
#GET_TOKENS = os.getenv('GET_TOKENS').split(",")


class PodcastProducer(threading.Thread):
    def __init__(self, jobs_q, purgatory_q, errors_q, quarantine_q, topic, producer, thread_lock, nlp, *args,
                 **kwargs):
        self.logger = ErrorLogger(thread_lock, errors_q, purgatory_q, quarantine_q)
        self.jobs_q = jobs_q
        self.nlp = nlp
        self.topic = topic
        self.producer = producer
        self.namespace = uuid.uuid5(uuid.NAMESPACE_DNS, UUID_NAMESPACE)
        self.redis_cli = redis.Redis(host=REDIS_HOST, port=6379, charset="utf-8", decode_responses=True)
        self.entity_type = 'podcast'
        super().__init__(*args, **kwargs)

    def run(self):
        while True:
            try:
                task = self.jobs_q.get(timeout=30)
                self.process(task)
            except queue.Empty:
                return

    def drop_on_kafka(self, message, topic):
        kafka_message = str(message).encode()
        self.producer.produce(topic=topic, key=str(uuid.uuid4()), value=kafka_message,
                              on_delivery=self.delivery_report)
        self.producer.flush()

    @staticmethod
    def delivery_report(errmsg, msg):
        if errmsg is not None:
            raise KafkaException(errmsg)

    @staticmethod
    def get_path(path_uuid):
        return f"{path_uuid[-1:]}/{path_uuid[-2:]}/{path_uuid[-3:]}/{path_uuid[-4:]}/{path_uuid}"

    @staticmethod
    def validate_minimums(msg):
        try:
            description_len = len(msg['description_cleaned'].split(' '))
            title_len = len(msg['title_cleaned'].split(' '))
            if description_len < MIN_DESCRIPTION_LENGTH or title_len < MIN_TITLE_LENGTH:
                raise TypeError(
                    f"Minimum length(s) not met: title {title_len}:{MIN_TITLE_LENGTH}, description {description_len}:{MIN_DESCRIPTION_LENGTH}.")
        except Exception:
            raise

    def process(self, record):
        message = {}
        try:
            # Make sure all required elements are present
            for element in REQUIRED_ELEMENTS:
                if element not in record:
                    raise TypeError(f"Record is missing required element: {element}.")

            message = {
                "original_url": record['rss'],
                "is_explicit": bool(record['explicit']),
                "podcast_uuid": str(uuid.uuid5(self.namespace, record['rss'])),
                "publisher": self.nlp.clean_text(record['publisher']),
                "description_cleaned": self.nlp.clean_text(record['description']),
                "title_cleaned": self.nlp.clean_text(record['title']),
                "episode_count": record['episode_count'],
                "readability": 0,
                "listen_score_global": float(record['listen_score_global_rank'].replace('%', 'e-2'))
                if record['listen_score_global_rank'] else 0,
                "description_selected": 410,
                "advanced_popularity": 0}


            message["hash_record"] = hashlib.md5(str(message).encode()).hexdigest()
            message["hash_title"] = hashlib.md5(message['title_cleaned'].encode()).hexdigest()
            message["hash_description"] = hashlib.md5(message['description_cleaned'].encode()).hexdigest()

            self.validate_minimums(message)
            message["language"] = self.nlp.get_language(self.nlp(message['description_cleaned']))

            # Check for Previous Instance in Redis
            previous_hash_record = self.redis_cli.get(f"{self.entity_type}_{message['podcast_uuid']}")

            # Check for supported Languages
            if message['language'] not in LANGUAGES:
                raise TypeError(f"Language not supported: {message['language']}.")
            # Check for Exact Duplicates using hash of the message dictionary and UUID5 of Rss URL.
            elif previous_hash_record == message['hash_record']:
                raise TypeError(f"File {message['rss_url']} is a duplicate to: {previous_hash_record}.")
            # Same URL Different Body. title says "DELETED"??
            elif previous_hash_record:
                raise QuarantineError({"podcast_uuid": message['podcast_uuid'], "duplicate_file_name": record['rss'],
                                       "original_hash_record": previous_hash_record})
            else:
                self.redis_cli.set(f"{self.entity_type}_{message['podcast_uuid']}", message['hash_record'])
                message['image_url'] = record['artwork_thumbnail']
                # Podcast(**message)
                self.drop_on_kafka(message, self.topic)

        except TypeError as err:
            # print(traceback.format_exc())
            self.logger.log_to_purgatory(message, str(err))
        except QuarantineError as quarantine_obj:
            self.logger.log_to_quarantine(quarantine_obj.args[0])
        except ValueError as err:
            self.logger.log_to_purgatory(message, str(err))
        except KafkaException as err:
            # print(traceback.format_exc())
            self.logger.log_to_errors(message['podcast_uuid'], str(err), traceback.format_exc(), 510)
        except Exception as err:
            print(traceback.format_exc())
            self.logger.log_to_errors(message['podcast_uuid'], str(err), traceback.format_exc(), 510)
        # finally:
        # pass
