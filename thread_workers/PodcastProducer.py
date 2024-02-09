import os
import uuid
import queue
import hashlib
import traceback
import redis
import threading
from iso639 import Lang
from iso639.exceptions import InvalidLanguageValue
from confluent_kafka import KafkaException
from confluent_avro import AvroKeyValueSerde, SchemaRegistry
from confluent_avro.schema_registry import HTTPBasicAuth
from dotenv import load_dotenv
from Errors import ValidationError, QuarantineError
from logger.Logger import ErrorLogger

load_dotenv()

LANGUAGES = os.getenv('LANGUAGES').split(",")
REQUIRED_ELEMENTS = os.getenv('REQUIRED_ELEMENTS').split(",")
UUID_NAMESPACE = os.getenv('UUID_NAMESPACE')
MIN_DESCRIPTION_LENGTH = int(os.getenv('MIN_PODCAST_DESC_LENGTH'))
MIN_TITLE_LENGTH = int(os.getenv('MIN_PODCAST_TITLE_LENGTH'))
REDIS_HOST = os.getenv('REDIS_HOST')


class PodcastProducer(threading.Thread):
    def __init__(self, jobs_q, purgatory_q, errors_q, quarantine_q, topics, producer, thread_lock, text_processor, *args,
                 **kwargs):
        self.logger = ErrorLogger(thread_lock, errors_q, purgatory_q, quarantine_q)
        self.jobs_q = jobs_q
        self.topics = topics
        self.text_processor = text_processor
        self.producer = producer
        self.namespace = uuid.uuid5(uuid.NAMESPACE_DNS, UUID_NAMESPACE)
        self.redis_cli = redis.Redis(host=REDIS_HOST, port=6379, charset="utf-8", decode_responses=True)

        super().__init__(*args, **kwargs)

    def run(self):
        while True:
            try:
                task = self.jobs_q.get(timeout=30)
                self.process(task)
            except queue.Empty:
                return

    def delivery_report(self, errmsg, msg):
        if errmsg is not None:
            raise KafkaException(errmsg)

    def validate_minimums(self, msg):
        try:
            description_len = len(msg['description_cleaned'].split(' '))
            title_len = len(msg['title_cleaned'].split(' '))
            if description_len < MIN_DESCRIPTION_LENGTH or title_len < MIN_TITLE_LENGTH:
                raise ValidationError(
                    f"Minimum length(s) not met: title {title_len}:{MIN_TITLE_LENGTH}, description {description_len}:{MIN_DESCRIPTION_LENGTH}.")
        except Exception:
            raise

    def put_episodes(self, msg):
        episode_message = {
            "rss_url": msg['rss_url'],
            "language": msg['language'],
            "is_explicit": msg['is_explicit'],
            "podcast_uuid": msg['podcast_uuid'],
            "publisher": msg['publisher']}

        kafka_message = str(episode_message).encode()
        self.producer.produce(topic=self.topics['episodes'], key=str(uuid.uuid4()), value=kafka_message,
                              on_delivery=self.delivery_report)

    def process(self, record):
        try:
            # Make sure all required elements are present
            for element in REQUIRED_ELEMENTS:
                if element not in record:
                    raise ValidationError(f"Record is missing required element: {element}.")

            message = {"rss_url": record['rss'],
                       "language": record['language'],
                       "is_explicit": bool(record['explicit']),
                       "podcast_uuid": str(uuid.uuid5(self.namespace, record['rss'])),
                       "publisher": self.text_processor.return_clean_text(record['publisher']),
                       "image_url": record['artwork_thumbnail'],
                       "description_cleaned": self.text_processor.return_clean_text(record['description']),
                       "title_cleaned": self.text_processor.return_clean_text(record['title']),
                       "episode_count": record['episode_count'],
                       "readability": 0,
                       "listen_score_global": float(record['listen_score_global_rank'].replace('%', 'e-2'))
                       if record['listen_score_global_rank'] else 0,
                       "description_selected": 110,
                       "advanced_popularity": 1,
                       "record_hash": hashlib.md5(str(record).encode()).hexdigest()}

            # Check for Previous Instance in Redis
            previous_podcast_uuid = self.redis_cli.get(message['record_hash'])
            # Check for Exact Duplicates using hash of entire record string and hash of Rss URL.
            if previous_podcast_uuid == message['podcast_uuid']:
                raise ValidationError(
                    "File {} is a duplicate to: {}.".format(message['rss_url'], previous_podcast_uuid))

            # Same Body Different URL. title says "DELETED"??
            elif previous_podcast_uuid:
                raise QuarantineError(previous_podcast_uuid)
            # Set entry in Redis
            else:
                self.redis_cli.set(message['record_hash'], message['podcast_uuid'])

            iso = Lang(message['language'])
            message['language'] = iso.pt1
            if message['language'] not in LANGUAGES:
                raise ValidationError(f"Language not supported: {message['language']}.")
            self.validate_minimums(message)
            kafka_message = str(message).encode()
            self.producer.produce(topic=self.topics['podcasts'], key=str(uuid.uuid4()), value=kafka_message,
                                  on_delivery=self.delivery_report)
            if message['episode_count'] > 0:
                self.put_episodes(message)
            self.producer.poll(0)

        except InvalidLanguageValue as err:
            # print(traceback.format_exc())
            self.logger.log_to_purgatory(message, str(err))
        except ValidationError as err:
            # print(traceback.format_exc())
            self.logger.log_to_purgatory(message, str(err))
        except QuarantineError as previous_podcast_uuid:
            self.logger.log_to_quarantine(message, str(previous_podcast_uuid))
        except KafkaException as err:
            # print(traceback.format_exc())
            self.logger.log_to_errors(message['podcast_uuid'], err, traceback.format_exc(), 555)
        except Exception as err:
            print(traceback.format_exc())
            self.logger.log_to_errors(message['podcast_uuid'], err, traceback.format_exc(), 555)
        finally:
            pass
