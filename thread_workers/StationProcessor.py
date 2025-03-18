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

load_dotenv()

# Load System ENV VARS
FIELDS_TO_LEMMA = os.getenv('FIELDS_TO_LEMMA', '').split(",")
FIELDS_TO_VECTOR = os.getenv('SHOW_FIELDS_TO_VECTOR', '').split(",")
MIN_DESCRIPTION_LENGTH = int(os.getenv('MIN_SHOW_DESC_LENGTH', 10))
MIN_TITLE_LENGTH = int(os.getenv('MIN_SHOW_TITLE_LENGTH', 3))
LANGUAGES = os.getenv('LANGUAGES', '').split(",")
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
FLUSH_REDIS_ON_START = bool(os.getenv('FLUSH_REDIS_ON_START', False))

if FLUSH_REDIS_ON_START:
    redis_cli = redis.Redis(host=REDIS_HOST, port=6379, charset="utf-8", decode_responses=True)
    redis_cli.flushdb()  # Clear hash cache


class StationProcessor(threading.Thread):
    """
    A threading-based processor for handling station-related data.
    Processes station metadata, validates it, and stores information in Redis.
    """

    def __init__(self, job_queue: queue.Queue, good_queue: queue.Queue, errors_q: queue.Queue,
                 quarantine_q: queue.Queue, purgatory_q: queue.Queue, nlp, thread_lock: threading.Lock,
                 *args, **kwargs):
        """
        Initializes the StationProcessor class.

        Args:
            job_queue: Queue containing jobs to process
            good_queue: Queue for successfully processed stations
            errors_q: Queue for errors
            quarantine_q: Queue for quarantine
            purgatory_q: Queue for data requiring further validation
            nlp: NLP processing object
            thread_lock: Lock for thread-safe operations
        """
        super().__init__(*args, **kwargs)
        self.nlp = nlp
        self.thread_lock = thread_lock
        self.logger = ErrorLogger(thread_lock, errors_q, purgatory_q, quarantine_q)
        self.complete_queue = good_queue
        self.job_queue = job_queue
        self.redis = redis.Redis(host=REDIS_HOST, port=6379, charset="utf-8", decode_responses=True)
        self.entity_type = 'station'

    def run(self) -> None:
        """Main execution loop that continuously processes jobs from the queue."""
        while True:
            try:
                task = self.job_queue.get(timeout=1)
                self.process(task)
            except queue.Empty:
                return
            self.job_queue.task_done()

    def get_field_vectors(self, message: dict) -> None:
        """Generates vector embeddings for specified fields and adds them to the message."""
        try:
            for key in FIELDS_TO_VECTOR:
                vector_key = f"{key.split('_')[0]}_vector"
                message[vector_key] = pickle.dumps(self.nlp.get_vector(message[key]))
        except Exception as e:
            raise RuntimeError(f"Error generating vectors: {str(e)}")

    @staticmethod
    def validate_text_length(msg: dict) -> None:
        """Validates that title and description meet minimum length requirements."""
        try:
            description_len = len(msg.get('description_cleaned', '').split(' '))
            title_len = len(msg.get('title_cleaned', '').split(' '))
            if description_len < MIN_DESCRIPTION_LENGTH or title_len < MIN_TITLE_LENGTH:
                raise ValueError(
                    f"Minimum length(s) not met: title {title_len}/{MIN_TITLE_LENGTH}, "
                    f"description {description_len}/{MIN_DESCRIPTION_LENGTH}."
                )
        except Exception as e:
            raise ValueError(f"Error validating text length: {str(e)}")

    def get_field_lemmas(self, message: dict) -> None:
        """Extracts lemmas for specified fields in the message."""
        try:
            for key in FIELDS_TO_LEMMA:
                lemma_key = f"{key.split('_')[0]}_lemma"
                message[lemma_key] = self.nlp.get_lemma(message[key], message['language'])
        except Exception as e:
            raise RuntimeError(f"Error generating lemmas: {str(e)}")

    def process(self, job: dict) -> None:
        """Processes an individual station job, validating, cleaning, and logging the data."""
        job = {k: (v if v != '(null)' else '') for k, v in job.items()}  # Replace null placeholders
        station = {
            "station_uuid": job['station_uuid'],
            "is_explicit": job['is_explicit'],
            "is_searchable": job['is_searchable'],
            "index_status": 310,
            "advanced_popularity": 1,
            "title_cleaned": self.nlp.clean_text(job['station_name']),
            "language": job['language'],
            "category": job['category'],
            "genres": job['genres'],
            "tags": job['tags'],
            "format": job['format'],
            "markets": job['markets'],
            "call_sign": job['call_sign'],
            "description_cleaned": self.nlp.clean_text(job['description']),
            "image_url": job['image_url']
        }

        try:
            job_hash = hashlib.md5(str(job).encode()).hexdigest()
            previous_record_hash = self.redis.get(f"{self.entity_type}_{station['station_uuid']}")

            if station['language'] not in LANGUAGES:
                raise ValueError(f"Unsupported language: {station['language']}")
            elif previous_record_hash == job_hash:
                raise ValueError(f"Duplicate record: {station['station_uuid']}")
            elif previous_record_hash:
                raise QuarantineError({"station_uuid": station['station_uuid']})
            else:
                self.redis.set(f"{self.entity_type}_{station['station_uuid']}", job_hash)
                self.validate_text_length(station)
                self.get_field_lemmas(station)
                station["station_quality_id"] = job['station_id']
                with self.thread_lock:
                    self.complete_queue.put(station)
        except (ValueError, QuarantineError) as err:
            station["station_purgatory_id"] = job['station_id']
            self.logger.log_to_purgatory(station, str(err))
        except Exception as err:
            self.logger.log_to_errors(station['station_uuid'], str(err), traceback.format_exc(), 1)