import hashlib
import os
import pickle
import redis
import threading
import queue
import traceback
from typing import Dict, Any

import requests
from confluent_kafka import KafkaException
from dotenv import load_dotenv
from nlp.Grader import Grader
from logger.Logger import ErrorLogger
from urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

load_dotenv()

# Load System Environment Variables
READABILITY_FIELD = os.getenv('FIELD_FOR_READABILITY')
GRADABLE_LANGUAGES = os.getenv('GRADABLE_LANGUAGES', '').split(',')
FIELDS_TO_LEMMA = os.getenv('FIELDS_TO_LEMMA', '').split(',')
FIELDS_TO_VECTOR = os.getenv('PODCAST_FIELDS_TO_VECTOR', '').split(',')
REDIS_HOST = os.getenv('REDIS_HOST')
UPLOAD_BUCKET = os.getenv('UPLOAD_BUCKET')
IMAGE_MIME_TYPES = os.getenv('IMAGE_MIME_TYPES', '').split(',')
DEFAULT_IMAGE_PATH = os.getenv('DEFAULT_IMAGE_PATH')


class PodcastConsumer(threading.Thread):
    """
    A consumer thread that processes podcast messages from a job queue.
    Extracts relevant metadata, processes NLP tasks, validates input, and
    uploads podcast-related files (RSS and images) to a cloud storage bucket.
    """

    def __init__(self,
                 jobs_q: queue.Queue,
                 quality_q: queue.Queue,
                 errors_q: queue.Queue,
                 upload_q: queue.Queue,
                 thread_lock: threading.Lock,
                 nlp,
                 model,
                 *args,
                 **kwargs) -> None:
        """
        Initializes the PodcastConsumer.

        Args:
            jobs_q: Queue containing podcast processing jobs.
            quality_q: Queue for validated and processed podcast messages.
            errors_q: Queue for logging errors.
            upload_q: Queue for handling file uploads.
            thread_lock: Thread lock for safe concurrent access.
            nlp: NLP processing instance.
            model: Machine learning model for NLP tasks.
        """
        super().__init__(*args, **kwargs)
        self.nlp = nlp
        self.model = model
        self.thread_lock = thread_lock
        self.logger = ErrorLogger(thread_lock, errors_q)
        self.job_queue = jobs_q
        self.upload_q = upload_q
        self.quality_q = quality_q
        self.redis_cli = redis.Redis(host=REDIS_HOST, port=6379, charset="utf-8", decode_responses=True)

    def run(self) -> None:
        """
        Continuously processes jobs from the queue until it is empty.
        """
        while True:
            try:
                task = self.job_queue.get()
                self.process(task)
            except queue.Empty:
                break
            except Exception as e:
                self.logger.log_to_errors("Unknown", str(e), traceback.format_exc(), 500)

    def get_field_lemmas(self, message: Dict[str, Any]) -> None:
        """
        Extracts lemma representations for specified fields using NLP.

        Args:
            message: Dictionary containing podcast metadata.
        """
        try:
            for key in FIELDS_TO_LEMMA:
                lemma_key = f"{key.split('_')[0]}_lemma"
                message[lemma_key] = self.nlp.get_lemma(message[key], message['language'])
        except Exception as e:
            raise RuntimeError(f"Lemma extraction failed: {e}")

    def get_field_vectors(self, kafka_message: Dict[str, Any]) -> None:
        """
        Extracts and serializes vector representations for specified fields.

        Args:
            kafka_message: Dictionary containing podcast metadata.
        """
        try:
            for key in FIELDS_TO_VECTOR:
                vector_key = f"{key.split('_')[0]}_vector"
                kafka_message[vector_key] = pickle.dumps(self.nlp.get_vector(kafka_message[key], self.model))
        except Exception as e:
            raise RuntimeError(f"Vector extraction failed: {e}")

    @staticmethod
    def get_path(path_uuid: str) -> str:
        """
        Generates a structured path based on UUID.

        Args:
            path_uuid: Unique identifier for the podcast.

        Returns:
            A structured path string.
        """
        return f"{path_uuid[-1:]}/{path_uuid[-2:]}/{path_uuid[-3:]}/{path_uuid[-4:]}/{path_uuid}"

    def get_rss_path(self, message: Dict[str, Any]) -> str:
        """
        Checks the availability of an RSS URL and generates a structured file path.

        Args:
            message: Dictionary containing podcast metadata.

        Returns:
            The structured RSS URL or an error message.
        """
        try:
            rss_head = requests.head(message['original_url'], timeout=2, verify=False)
            if rss_head.status_code == 200:
                rss_message = {
                    'upload_bucket': UPLOAD_BUCKET,
                    'file_path': self.get_path(message['podcast_uuid']),
                    'file_name': f"{message['hash_record']}.rss",
                    'url': message['original_url'],
                    'content_type': 'rss',
                    'field': 'rss_url',
                    'mime_type': 'application/rss+xml',
                    'podcast_uuid': message['podcast_uuid']
                }
                with self.thread_lock:
                    self.upload_q.put(rss_message)
                return f"https://{UPLOAD_BUCKET}.s3.amazonaws.com/{self.get_path(message['podcast_uuid'])}/{message['hash_record']}.rss"
            else:
                return f"URL returned a {rss_head.status_code} status code"
        except Exception as e:
            return f"Failed to retrieve RSS path: {e}"

    def process(self, message: Dict[str, Any]) -> None:
        """
        Processes a podcast message, extracting and validating relevant fields.

        Args:
            message: Dictionary containing podcast metadata.
        """
        try:
            message["rss_url"] = self.get_rss_path(message)
            self.get_field_vectors(message)
            self.get_field_lemmas(message)

            if message['language'] in GRADABLE_LANGUAGES:
                message['readability'] = Grader.get_readability(message[READABILITY_FIELD])

            with self.thread_lock:
                self.quality_q.put(message)
        except Exception as err:
            self.logger.log_to_errors(message.get('original_url', 'Unknown'), str(err), traceback.format_exc(), 510)