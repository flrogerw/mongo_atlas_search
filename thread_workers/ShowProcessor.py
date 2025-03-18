import pickle
import os
import threading
import queue
import redis
import hashlib
import traceback
from dotenv import load_dotenv
from Errors import QuarantineError
from logger.Logger import ErrorLogger
from typing import Dict, Any

load_dotenv()

# Load System ENV VARS
FIELDS_TO_LEMMA = os.getenv('FIELDS_TO_LEMMA').split(",")
FIELDS_TO_VECTOR = os.getenv('STATION_FIELDS_TO_VECTOR').split(",")
MIN_DESCRIPTION_LENGTH = int(os.getenv('MIN_STATION_DESC_LENGTH'))
MIN_TITLE_LENGTH = int(os.getenv('MIN_STATION_TITLE_LENGTH'))
LANGUAGES = os.getenv('LANGUAGES').split(",")
REDIS_HOST = os.getenv('REDIS_HOST')
FLUSH_REDIS_ON_START = bool(os.getenv('FLUSH_REDIS_ON_START'))
READABILITY_FIELD = os.getenv('FIELD_FOR_READABILITY')

if FLUSH_REDIS_ON_START:
    redis_cli = redis.Redis(host=REDIS_HOST, port=6379, charset="utf-8", decode_responses=True)
    redis_cli.flushdb()  # Clear hash cache


class ShowProcessor(threading.Thread):
    """
    A threaded processor for handling show data, applying NLP transformations, and managing deduplication using Redis.
    """

    def __init__(
            self,
            job_queue: queue.Queue,
            good_queue: queue.Queue,
            errors_q: queue.Queue,
            quarantine_q: queue.Queue,
            purgatory_q: queue.Queue,
            nlp,
            thread_lock: threading.Lock,
            model,
            *args,
            **kwargs
    ) -> None:
        """
        Initializes the ShowProcessor class.

        Args:
            job_queue: Queue containing show metadata tasks.
            good_queue: Queue for successfully processed shows.
            errors_q: Queue for error messages.
            quarantine_q: Queue for quarantined records.
            purgatory_q: Queue for storing purgatory messages.
            nlp: NLP processor instance.
            thread_lock: Threading lock for safe multithreading.
            model: NLP model instance.
        """
        super().__init__(*args, **kwargs)
        self.model = model
        self.nlp = nlp
        self.thread_lock = thread_lock
        self.logger = ErrorLogger(thread_lock, errors_q, purgatory_q, quarantine_q)
        self.complete_queue = good_queue
        self.job_queue = job_queue
        self.redis = redis.Redis(host=REDIS_HOST, port=6379, charset="utf-8", decode_responses=True)
        self.entity_type = 'show'

    def run(self) -> None:
        """
        Continuously processes jobs from the queue until it's empty.
        """
        while True:
            try:
                task = self.job_queue.get(timeout=1)
                self.process(task)
                self.job_queue.task_done()
            except queue.Empty:
                return

    def get_field_vectors(self, message: Dict[str, Any]) -> None:
        """
        Computes and stores vector representations for specified fields.

        Args:
            message: Dictionary containing show metadata.
        """
        try:
            for key in FIELDS_TO_VECTOR:
                vector_key = f"{key.split('_')[0]}_vector"
                message[vector_key] = pickle.dumps(self.nlp.get_vector(message[key], self.model))
        except Exception as e:
            raise RuntimeError(f"Error computing field vectors: {e}")

    @staticmethod
    def validate_text_length(msg: Dict[str, Any]) -> None:
        """
        Validates minimum text length for title and description.

        Args:
            msg: Dictionary containing show metadata.
        """
        try:
            description_len = len(msg['description_cleaned'].split(' '))
            title_len = len(msg['title_cleaned'].split(' '))
            if description_len < MIN_DESCRIPTION_LENGTH or title_len < MIN_TITLE_LENGTH:
                raise ValueError(
                    f"Minimum length(s) not met: title {title_len}:{MIN_TITLE_LENGTH}, description {description_len}:{MIN_DESCRIPTION_LENGTH}."
                )
        except Exception as e:
            raise RuntimeError(f"Error validating text length: {e}")

    def get_field_lemmas(self, message: Dict[str, Any]) -> None:
        """
        Generates and stores lemma representations for specified fields.

        Args:
            message: Dictionary containing show metadata.
        """
        try:
            for key in FIELDS_TO_LEMMA:
                lemma_key = f"{key.split('_')[0]}_lemma"
                message[lemma_key] = self.nlp.get_lemma(message[key], message['language'])
        except Exception as e:
            raise RuntimeError(f"Error computing field lemmas: {e}")

    def process(self, job: Dict[str, Any]) -> None:
        """
        Processes an individual show record, applying NLP transformations and deduplication logic.

        Args:
            job: Dictionary containing show metadata.
        """
        try:
            job = {k: (v if v != '(null)' else '') for k, v in job.items()}
            show = {
                "show_uuid": job['show_uuid'],
                "is_explicit": False,
                "is_searchable": True,
                "index_status": 310,
                "advanced_popularity": 1,
                "language": 'en',
                "title_cleaned": self.nlp.clean_text(job['name']),
                "description_cleaned": self.nlp.clean_text(job['description']),
                "image_url": job['media_url'],
                "stations": job['station_list'],
                "tags": str(
                    [self.nlp.get_lemma(w.lower(), 'en') for w in self.nlp.clean_text(job['keywords']).split(',')]
                )
            }

            job_hash = hashlib.md5(str(job).encode()).hexdigest()
            previous_record_hash = self.redis.get(f"{self.entity_type}_{show['show_uuid']}")

            if show['language'] not in LANGUAGES:
                raise ValueError(f"Language not supported: {show['language']}.")
            elif previous_record_hash == job_hash:
                raise ValueError(f"File is a duplicate: {previous_record_hash}.")
            elif previous_record_hash:
                raise QuarantineError({"show_uuid": show['show_uuid'], "original_show_uuid": show['show_uuid']})
            else:
                self.redis.set(f"{self.entity_type}_{show['show_uuid']}", job_hash)
                self.validate_text_length(show)
                self.get_field_lemmas(show)
                with self.thread_lock:
                    self.complete_queue.put(show)
        except ValueError as err:
            self.logger.log_to_purgatory(show, str(err))
        except QuarantineError as quarantine_obj:
            self.logger.log_to_quarantine(quarantine_obj.args[0])
        except Exception as err:
            self.logger.log_to_errors(show.get('show_uuid', 'Unknown'), str(err), traceback.format_exc(), 1)
