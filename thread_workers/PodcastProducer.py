import os
import uuid
import queue
import hashlib
import traceback
import requests
import redis
import threading
from confluent_kafka import KafkaException
from dotenv import load_dotenv
from Errors import QuarantineError
from logger.Logger import ErrorLogger
from schemas.validation import Podcast
from urllib3.exceptions import InsecureRequestWarning
from typing import Dict, Any

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

# Load environment variables
load_dotenv()
LANGUAGES = os.getenv('LANGUAGES').split(",")
REQUIRED_ELEMENTS = os.getenv('REQUIRED_ELEMENTS').split(",")
UUID_NAMESPACE = os.getenv('UUID_NAMESPACE')
MIN_DESCRIPTION_LENGTH = int(os.getenv('MIN_PODCAST_DESC_LENGTH'))
MIN_TITLE_LENGTH = int(os.getenv('MIN_PODCAST_TITLE_LENGTH'))
REDIS_HOST = os.getenv('REDIS_HOST')


class PodcastProducer(threading.Thread):
    """
    A Kafka producer that processes podcast metadata and publishes it to a Kafka topic.
    """

    def __init__(self, jobs_q: queue.Queue, purgatory_q: queue.Queue, errors_q: queue.Queue,
                 quarantine_q: queue.Queue, topic: str, producer, thread_lock: threading.Lock, nlp, *args, **kwargs) -> None:
        """
        Initializes the PodcastProducer class.

        Args:
            jobs_q: Queue containing podcast metadata tasks.
            purgatory_q: Queue for storing purgatory messages.
            errors_q: Queue for storing error messages.
            quarantine_q: Queue for quarantined records.
            topic: Kafka topic for publishing messages.
            producer: Kafka producer instance.
            thread_lock: Threading lock for safe multithreading.
            nlp: NLP processor instance.
        """
        self.logger = ErrorLogger(thread_lock, errors_q, purgatory_q, quarantine_q)
        self.jobs_q = jobs_q
        self.nlp = nlp
        self.topic = topic
        self.producer = producer
        self.namespace = uuid.uuid5(uuid.NAMESPACE_DNS, UUID_NAMESPACE)
        self.redis_cli = redis.Redis(host=REDIS_HOST, port=6379, charset="utf-8", decode_responses=True)
        self.entity_type = 'podcast'
        super().__init__(*args, **kwargs)

    def run(self) -> None:
        """
        Continuously processes tasks from the jobs queue until it's empty.
        """
        while True:
            try:
                task = self.jobs_q.get(timeout=30)
                self.process(task)
            except queue.Empty:
                return

    def drop_on_kafka(self, message: Dict[str, Any], topic: str) -> None:
        """
        Publishes a message to the Kafka topic.

        Args:
            message: Dictionary containing podcast metadata.
            topic: Kafka topic name.
        """
        try:
            kafka_message = str(message).encode()
            self.producer.produce(topic=topic, key=str(uuid.uuid4()), value=kafka_message,
                                  on_delivery=self.delivery_report)
            self.producer.flush()
        except KafkaException as err:
            raise KafkaException(err)

    @staticmethod
    def delivery_report(errmsg, msg) -> None:
        """
        Handles Kafka message delivery reports.

        Args:
            errmsg: Error message if delivery fails.
            msg: Kafka message.
        """
        if errmsg is not None:
            raise KafkaException(errmsg)

    @staticmethod
    def get_path(path_uuid: str) -> str:
        """
        Generates a structured path based on a UUID.

        Args:
            path_uuid: UUID string.

        Returns:
            Formatted path string.
        """
        return f"{path_uuid[-1:]}/{path_uuid[-2:]}/{path_uuid[-3:]}/{path_uuid[-4:]}/{path_uuid}"

    @staticmethod
    def validate_minimums(msg: Dict[str, Any]) -> None:
        """
        Validates minimum required fields for podcast metadata.

        Args:
            msg: Dictionary containing podcast metadata.

        Raises:
            TypeError: If title or description length is insufficient.
        """
        try:
            description_len = len(msg['description_cleaned'].split(' '))
            title_len = len(msg['title_cleaned'].split(' '))
            if description_len < MIN_DESCRIPTION_LENGTH or title_len < MIN_TITLE_LENGTH:
                raise TypeError(
                    f"Minimum length(s) not met: title {title_len}:{MIN_TITLE_LENGTH}, description {description_len}:{MIN_DESCRIPTION_LENGTH}.")
        except Exception as e:
            raise TypeError(f"Validation error: {str(e)}")

    def process(self, record: Dict[str, Any]) -> None:
        """
        Processes a podcast record and publishes it to Kafka if valid.

        Args:
            record: Dictionary containing podcast metadata.
        """
        message = {}
        try:
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
                "genres": record['genres'],
                "tags": '',
                "language": record['language'],
                "listen_score_global": float(record['listen_score_global_rank'].replace('%', 'e-2'))
                if record['listen_score_global_rank'] else 0,
                "description_selected": 410,
                "advanced_popularity": 0}

            message["hash_record"] = hashlib.md5(str(message).encode()).hexdigest()
            self.validate_minimums(message)
            message["language"] = self.nlp.get_language(self.nlp(message['description_cleaned']))

            previous_hash_record = self.redis_cli.get(f"{self.entity_type}_{message['podcast_uuid']}")

            if message['language'] not in LANGUAGES:
                raise TypeError(f"Language not supported: {message['language']}.")
            elif previous_hash_record == message['hash_record']:
                raise TypeError(f"Duplicate record detected: {previous_hash_record}.")
            elif previous_hash_record:
                raise QuarantineError({"podcast_uuid": message['podcast_uuid'], "duplicate_file_name": record['rss'],
                                       "original_hash_record": previous_hash_record})
            else:
                self.redis_cli.set(f"{self.entity_type}_{message['podcast_uuid']}", message['hash_record'])
                message['image_url'] = record['artwork_thumbnail']
                self.drop_on_kafka(message, self.topic)
        except (TypeError, ValueError, KafkaException, QuarantineError) as err:
            self.logger.log_to_errors(message.get('podcast_uuid', 'UNKNOWN'), str(err), traceback.format_exc(), 510)
        except Exception as err:
            self.logger.log_to_errors(message.get('podcast_uuid', 'UNKNOWN'), str(err), traceback.format_exc(), 510)
