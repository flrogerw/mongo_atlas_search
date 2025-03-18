import os
import pickle
import uuid
import threading
import queue
import traceback
import requests
import hashlib
import redis
from bs4 import BeautifulSoup
from dateutil import parser
from dotenv import load_dotenv
from pydantic import ValidationError
from nlp.Grader import Grader
from logger.Logger import ErrorLogger
from schemas.validation import Episode
from Errors import QuarantineError
from typing import Dict, Any, Optional

# Load environment variables
load_dotenv()
READABILITY_FIELD: str = os.getenv('FIELD_FOR_READABILITY', '')
GRADABLE_LANGUAGES: list = os.getenv('GRADABLE_LANGUAGES', "").split(",")
FIELDS_TO_LEMMA: list = os.getenv('FIELDS_TO_LEMMA', "").split(",")
FIELDS_TO_VECTOR: list = os.getenv('EPISODE_FIELDS_TO_VECTOR', "").split(",")
MIN_DESCRIPTION_LENGTH: int = int(os.getenv('MIN_PODCAST_DESC_LENGTH', 10))
MIN_TITLE_LENGTH: int = int(os.getenv('MIN_PODCAST_TITLE_LENGTH', 3))
REDIS_HOST: str = os.getenv('REDIS_HOST', 'localhost')
UUID_NAMESPACE: str = os.getenv('UUID_NAMESPACE', 'default-namespace')
TRUE_BOOL: set = set(os.getenv('TRUE_BOOL', "true,yes,1").split(","))
FALSE_BOOL: set = set(os.getenv('FALSE_BOOL', "false,no,0").split(","))


class EpisodeConsumer(threading.Thread):
    """
    A consumer thread for processing podcast episode data from an RSS feed.
    This class pulls messages from a queue, validates, processes, and enriches them before storing.

    Args:
        jobs_q (queue.Queue): Queue holding incoming jobs to be processed.
        quality_q (queue.Queue): Queue to store validated and processed episodes.
        errors_q (queue.Queue): Queue to store errors encountered during processing.
        purgatory_q (queue.Queue): Queue for temporarily held data with potential issues.
        quarantine_q (queue.Queue): Queue for storing quarantined episodes (duplicates, violations).
        thread_lock (threading.Lock): Thread lock for safe logging operations.
        nlp: NLP model for text processing (lemmatization, vectorization).
        model: Machine learning model for text vector extraction.
    """

    def __init__(
        self,
        jobs_q: queue.Queue,
        quality_q: queue.Queue,
        errors_q: queue.Queue,
        purgatory_q: queue.Queue,
        quarantine_q: queue.Queue,
        thread_lock: threading.Lock,
        nlp,
        model,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.nlp = nlp
        self.model = model
        self.job_queue = jobs_q
        self.quality_q = quality_q
        self.thread_lock = thread_lock
        self.logger = ErrorLogger(thread_lock, errors_q, purgatory_q, quarantine_q)
        self.redis_cli = redis.Redis(host=REDIS_HOST, port=6379, charset="utf-8", decode_responses=True)
        self.namespace = uuid.uuid5(uuid.NAMESPACE_DNS, UUID_NAMESPACE)
        self.entity_type = 'episode'

    def run(self) -> None:
        """
        Continuously process tasks from the job queue until the queue is empty.
        """
        while True:
            try:
                task = self.job_queue.get(timeout=30)
                self.process(task)
            except queue.Empty:
                return

    @staticmethod
    def fetch_rss(file_path: str) -> str:
        """
        Fetches and returns RSS feed content as a string.

        Args:
            file_path (str): URL or path to the RSS feed.

        Returns:
            str: The RSS feed content.

        Raises:
            RuntimeError: If fetching the RSS feed fails.
        """
        try:
            response = requests.get(file_path, timeout=2.0)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            raise RuntimeError(f"Failed to fetch RSS: {e}")

    def validate_minimums(self, msg: Dict[str, Any]) -> None:
        """
        Ensures episode title and description meet minimum length requirements.

        Args:
            msg (Dict[str, Any]): The episode data dictionary.

        Raises:
            ValueError: If the title or description does not meet the minimum length.
        """
        description_len = len(msg['description_cleaned'].split())
        title_len = len(msg['title_cleaned'].split())

        if description_len < MIN_DESCRIPTION_LENGTH or title_len < MIN_TITLE_LENGTH:
            raise ValueError(
                f"Minimum lengths not met: title {title_len}/{MIN_TITLE_LENGTH}, "
                f"description {description_len}/{MIN_DESCRIPTION_LENGTH}."
            )

    def extract_lemmas(self, message: Dict[str, Any]) -> None:
        """
        Extracts lemmas for configured fields using NLP processing.

        Args:
            message (Dict[str, Any]): The episode data dictionary.
        """
        for key in FIELDS_TO_LEMMA:
            lemma_key = f"{key.split('_')[0]}_lemma"
            message[lemma_key] = self.nlp.get_lemma(message[key], message['language'])

    def extract_vectors(self, kafka_message: Dict[str, Any]) -> None:
        """
        Extracts vector embeddings for configured fields using NLP model.

        Args:
            kafka_message (Dict[str, Any]): The episode data dictionary.
        """
        for key in FIELDS_TO_VECTOR:
            vector_key = f"{key.split('_')[0]}_vector"
            kafka_message[vector_key] = pickle.dumps(self.nlp.get_vector(kafka_message[key], self.model))

    def to_bool(self, value: str, default: bool) -> bool:
        """
        Converts string representations of boolean values to actual boolean values.

        Args:
            value (str): String representation of a boolean value.
            default (bool): Default boolean value if conversion fails.

        Returns:
            bool: Converted boolean value.
        """
        if isinstance(value, bool):
            return value
        value = value.lower()
        if value in TRUE_BOOL:
            return True
        elif value in FALSE_BOOL:
            return False
        return default

    def get_guid(self, guid_str: str) -> str:
        """
        Generates a UUID if the given string is not already a valid UUID.

        Args:
            guid_str (str): The input string to be converted into a UUID.

        Returns:
            str: A valid UUID string.
        """
        try:
            uuid.UUID(str(guid_str))
            return guid_str
        except ValueError:
            return str(uuid.uuid5(self.namespace, guid_str))

    def process(self, kafka_message: Dict[str, Any]) -> None:
        """
        Processes an episode message, validates it, and extracts necessary data.

        Args:
            kafka_message (Dict[str, Any]): The episode data dictionary.

        Raises:
            ValueError: If the episode message contains invalid data.
            QuarantineError: If a duplicate episode is detected.
            Exception: Logs errors encountered during processing.
        """
        try:
            rss_content = self.fetch_rss(kafka_message['rss_url'])
            root = BeautifulSoup(rss_content, features="xml")

            for item in root.findAll("item"):
                episode_message = self.extract_episode_data(item, kafka_message)
                self.validate_minimums(episode_message)

                redis_key = f"{self.entity_type}_{episode_message['episode_uuid']}"
                previous_episode_hash = self.redis_cli.get(redis_key)

                if previous_episode_hash == episode_message['episode_uuid']:
                    raise ValueError(f"Duplicate episode detected: {episode_message['episode_url']}.")
                elif previous_episode_hash:
                    raise QuarantineError({
                        "episode_uuid": episode_message['episode_uuid'],
                        "duplicate_file_name": episode_message['episode_url'],
                        "original_episode_hash": previous_episode_hash
                    })
                else:
                    self.redis_cli.set(redis_key, episode_message['record_hash'])

                if episode_message['language'] in GRADABLE_LANGUAGES and len(episode_message.get(READABILITY_FIELD, "")) > 5:
                    episode_message['readability'] = Grader.get_readability(episode_message[READABILITY_FIELD])

                self.extract_vectors(episode_message)
                self.extract_lemmas(episode_message)

                with self.thread_lock:
                    self.quality_q.put(episode_message)
        except (ValueError, ValidationError) as err:
            self.logger.log_to_purgatory(kafka_message.get('rss_url', 'Unknown RSS'), str(err))
        except QuarantineError as quarantine_obj:
            self.logger.log_to_quarantine(quarantine_obj.args[0])
        except Exception as err:
            self.logger.log_to_errors(kafka_message.get('rss_url', 'Unknown RSS'), str(err), traceback.format_exc(), 520)
