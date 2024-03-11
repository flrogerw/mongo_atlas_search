import os
import uuid
import queue
import hashlib
import traceback
import requests
import redis
import threading
from iso639 import Lang
from iso639.exceptions import InvalidLanguageValue
from confluent_kafka import KafkaException
# from confluent_avro import AvroKeyValueSerde, SchemaRegistry
# from confluent_avro.schema_registry import HTTPBasicAuth
from dotenv import load_dotenv
from Errors import ValidationError, QuarantineError
from logger.Logger import ErrorLogger
from schemas.validation import Podcast

load_dotenv()
LANGUAGES = os.getenv('LANGUAGES').split(",")
REQUIRED_ELEMENTS = os.getenv('REQUIRED_ELEMENTS').split(",")
UUID_NAMESPACE = os.getenv('UUID_NAMESPACE')
MIN_DESCRIPTION_LENGTH = int(os.getenv('MIN_PODCAST_DESC_LENGTH'))
MIN_TITLE_LENGTH = int(os.getenv('MIN_PODCAST_TITLE_LENGTH'))
REDIS_HOST = os.getenv('REDIS_HOST')
IMAGE_MIME_TYPES = os.getenv('IMAGE_MIME_TYPES').split(",")
KAFKA_UPLOAD_TOPIC = os.getenv('KAFKA_UPLOAD_TOPIC')
DEFAULT_IMAGE_PATH = os.getenv('DEFAULT_IMAGE_PATH')
UPLOAD_BUCKET = os.getenv('UPLOAD_BUCKET')


class PodcastProducer(threading.Thread):
    def __init__(self, jobs_q, purgatory_q, errors_q, quarantine_q, topic, producer, thread_lock, text_processor, *args,
                 **kwargs):
        self.logger = ErrorLogger(thread_lock, errors_q, purgatory_q, quarantine_q)
        self.jobs_q = jobs_q
        self.topic = topic
        self.text_processor = text_processor
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

    def delivery_report(self, errmsg, msg):
        if errmsg is not None:
            raise KafkaException(errmsg)

    def get_path(self, path_uuid):
        return f"{path_uuid[-1:]}/{path_uuid[-2:]}/{path_uuid[-3:]}/{path_uuid[-4:]}/{path_uuid}"

    def get_rss_path(self, message):
        rss_head = requests.head(message['original_url'], timeout=2, verify=False)
        if rss_head.status_code == 200:
            rss_message = {
                'upload_bucket': UPLOAD_BUCKET,
                'file_path': self.get_path(message['podcast_uuid']),
                'file_name': f"{message['record_hash']}.rss",
                'url': message['original_url'],
                'content_type': 'rss',
                'field': 'rss_url',
                'mime_type': 'application/rss+xml',
                'podcast_uuid': message['podcast_uuid']
            }
            self.drop_on_kafka(rss_message, KAFKA_UPLOAD_TOPIC)
            file_path = f"https://{UPLOAD_BUCKET}.s3.amazonaws.com/{self.get_path(message['podcast_uuid'])}/{message['record_hash']}.rss"
        else:
            file_path = f"URL returned a {rss_head.status_code} status code"
        return file_path

    def get_image_path(self, image_url, parent_uuid):
        try:
            image_head = requests.head(image_url, verify=False, timeout=2.0)
            if image_head.status_code == 200 and image_head.headers['Content-Type'] in IMAGE_MIME_TYPES:
                _, extension = image_head.headers["content-type"].split("/")  # Cheesy way.  Needs better
                image_name = hashlib.md5(str(image_url).encode()).hexdigest()
                previous_image = self.redis_cli.get(f"image_{image_name}")
                if previous_image is None:
                    path = self.get_path(parent_uuid)
                    image_path = f"{path}/{image_name}.{extension}"
                    self.redis_cli.set(f"image_{image_name}", image_path)
                    image_message = {
                        'upload_bucket': UPLOAD_BUCKET,
                        'file_path': path,
                        'file_name': f"{image_name}.{extension}",
                        'url': image_url,
                        'content_type': 'image',
                        'field': 'image_url',
                        'mime_type': image_head.headers['Content-Type'],
                        'podcast_uuid': parent_uuid
                    }
                    self.drop_on_kafka(image_message, KAFKA_UPLOAD_TOPIC)
                else:
                    image_path = previous_image
            else:
                image_path = DEFAULT_IMAGE_PATH
        except Exception as err:
            # print(traceback.format_exc())
            image_path = DEFAULT_IMAGE_PATH
            pass
        finally:
            return f"https://{UPLOAD_BUCKET}.s3.amazonaws.com/{image_path}"

    def validate_minimums(self, msg):
        try:
            description_len = len(msg['description_cleaned'].split(' '))
            title_len = len(msg['title_cleaned'].split(' '))
            if description_len < MIN_DESCRIPTION_LENGTH or title_len < MIN_TITLE_LENGTH:
                raise TypeError(
                    f"Minimum length(s) not met: title {title_len}:{MIN_TITLE_LENGTH}, description {description_len}:{MIN_DESCRIPTION_LENGTH}.")
        except Exception:
            raise

    def process(self, record):
        try:
            # Make sure all required elements are present
            for element in REQUIRED_ELEMENTS:
                if element not in record:
                    raise TypeError(f"Record is missing required element: {element}.")
            message = {
                "original_url": record['rss'],
                "language": record['language'],
                "is_explicit": bool(record['explicit']),
                "podcast_uuid": str(uuid.uuid5(self.namespace, record['rss'])),
                "publisher": self.text_processor.return_clean_text(record['publisher']),
                "description_cleaned": self.text_processor.return_clean_text(record['description']),
                "title_cleaned": self.text_processor.return_clean_text(record['title']),
                "episode_count": record['episode_count'],
                "readability": 0,
                "listen_score_global": float(record['listen_score_global_rank'].replace('%', 'e-2'))
                if record['listen_score_global_rank'] else 0,
                "description_selected": 410,
                "record_hash": hashlib.md5(str(record).encode()).hexdigest(),
                "advanced_popularity": 1}

            # Check for Previous Instance in Redis
            previous_podcast_uuid = self.redis_cli.get(f"{self.entity_type}_{message['record_hash']}")
            # Check for Exact Duplicates using hash of entire record string and hash of Rss URL.
            if previous_podcast_uuid == message['podcast_uuid']:
                raise TypeError(f"File {message['rss_url']} is a duplicate to: {previous_podcast_uuid}.")

            # Same Body Different URL. title says "DELETED"??
            elif previous_podcast_uuid:
                raise QuarantineError({"podcast_uuid": message['podcast_uuid'], "duplicate_file_name": record['rss'],
                                       "original_podcast_uuid": previous_podcast_uuid})
            # Set entry in Redis
            else:
                self.validate_minimums(message)

                self.redis_cli.set(f"{self.entity_type}_{message['record_hash']}", message['podcast_uuid'])
                iso = Lang(message['language'])
                message['language'] = iso.pt1
                if message['language'] not in LANGUAGES:
                    raise TypeError(f"Language not supported: {message['language']}.")
                message['image_url'] = self.get_image_path(record['artwork_thumbnail'], message['podcast_uuid'])
                message["rss_url"] = self.get_rss_path(message)
                Podcast(**message)
                self.drop_on_kafka(message, self.topic)
                # Create upload object
                kafka_rss_message = {
                    'upload_bucket': UPLOAD_BUCKET,
                    'file_path': self.get_path(message['podcast_uuid']),
                    'file_name': f"{message['record_hash']}.rss",
                    'url': record['rss'],
                    'content_type': 'rss',
                    'field': 'rss_url',
                    'mime_type': 'application/rss+xml',
                    'podcast_uuid': message['podcast_uuid']
                }
                self.drop_on_kafka(kafka_rss_message, KAFKA_UPLOAD_TOPIC)

        except InvalidLanguageValue as err:
            # print(traceback.format_exc())
            self.logger.log_to_purgatory(message, str(err))
        except TypeError as err:
            # print(traceback.format_exc())
            self.logger.log_to_purgatory(message, str(err))
        except QuarantineError as quarantine_obj:
            self.logger.log_to_quarantine(quarantine_obj)
        except ValidationError as err:
            self.logger.log_to_purgatory(message, str(err.errors()))
        except KafkaException as err:
            # print(traceback.format_exc())
            self.logger.log_to_errors(message['podcast_uuid'], str(err), traceback.format_exc(), 2)
        except Exception as err:
            # print(traceback.format_exc())
            self.logger.log_to_errors(message['podcast_uuid'], str(err), traceback.format_exc(), 2)
        # finally:
        # pass
