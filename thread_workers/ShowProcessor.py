import pickle

from Errors import QuarantineError
import os
import threading
import queue
import redis
import hashlib
import traceback
from dotenv import load_dotenv
from logger.Logger import ErrorLogger

load_dotenv()

# Load System ENV VARS
FIELDS_TO_LEMMA = os.getenv('FIELDS_TO_LEMMA').split(",")
FIELDS_TO_VECTOR = os.getenv('STATION_FIELDS_TO_VECTOR').split(",")
MIN_DESCRIPTION_LENGTH = int(os.getenv('MIN_STATION_DESC_LENGTH'))
MIN_TITLE_LENGTH = int(os.getenv('MIN_STATION_TITLE_LENGTH'))
LANGUAGES = os.getenv('LANGUAGES').split(",")
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_USER = os.getenv('REDIS_USER')
REDIS_PASS = os.getenv('REDIS_PASS')
FLUSH_REDIS_ON_START = bool(os.getenv('FLUSH_REDIS_ON_START'))
READABILITY_FIELD = os.getenv('FIELD_FOR_READABILITY')

if FLUSH_REDIS_ON_START:
    redis_cli = redis.Redis(host=REDIS_HOST,
                            port=6379,
                            charset="utf-8",
                            decode_responses=True)
    redis_cli.flushdb()  # Clear hash cache


class ShowProcessor(threading.Thread):
    def __init__(self, job_queue, good_queue, errors_q, quarantine_q, purgatory_q, nlp, thread_lock,
                 model, *args, **kwargs):
        self.model = model
        self.nlp = nlp
        self.thread_lock = thread_lock
        self.logger = ErrorLogger(thread_lock, errors_q, purgatory_q, quarantine_q)
        self.complete_queue = good_queue
        self.job_queue = job_queue
        self.redis = redis.Redis(host=REDIS_HOST, port=6379, charset="utf-8", decode_responses=True)
        self.entity_type = 'show'
        super().__init__(*args, **kwargs)

    def run(self):
        while True:
            try:
                task = self.job_queue.get(timeout=1)
                self.process(task)
            except queue.Empty:
                return
            self.job_queue.task_done()

    def get_field_vectors(self, message):
        try:
            for key in FIELDS_TO_VECTOR:
                vector_key = f"{key.split('_')[0]}_vector"
                message[vector_key] = pickle.dumps(self.nlp.get_vector(message[key], self.model))
        except Exception:
            raise

    @staticmethod
    def validate_text_length(msg):
        try:
            description_len = len(msg['description_cleaned'].split(' '))
            title_len = len(msg['title_cleaned'].split(' '))
            if description_len < MIN_DESCRIPTION_LENGTH or title_len < MIN_TITLE_LENGTH:
                raise TypeError(
                    f"Minimum length(s) not met: title {title_len}:{MIN_TITLE_LENGTH}, description {description_len}:{MIN_DESCRIPTION_LENGTH}.")
        except Exception:
            raise

    def get_field_lemmas(self, message):
        try:
            for key in FIELDS_TO_LEMMA:
                lemma_key = f"{key.split('_')[0]}_lemma"
                message[lemma_key] = self.nlp.get_lemma(message[key], message['language'])
        except Exception:
            raise

    def process(self, job):
        job = dict((k, v if v != '(null)' else '') for k, v in job.items())
        show = {
            "show_uuid": job['show_uuid'],
            "is_explicit": False,
            "is_searchable": True,
            "index_status": 310,
            "advanced_popularity": 1,
            "language": 'en',  # data to sparse to determine
            "title_cleaned": self.nlp.clean_text(job['name']),
            "description_cleaned": self.nlp.clean_text(job['description']),
            "image_url": job['media_url'],
            "stations": job['station_list'],
            "tags": str(list(map(lambda w: self.nlp.get_lemma(w.lower(), 'en'), self.nlp.clean_text(job['keywords']).split(','))))
        }

        try:
            job_hash = hashlib.md5(str(job).encode()).hexdigest()
            previous_record_hash = self.redis.get(f"{self.entity_type}_{show['show_uuid']}")
            """
            if len(show['description_cleaned']) > 25:
                show["language"] = self.nlp.get_language(self.nlp(show['description_cleaned']))
            """

            # Check for supported Languages
            if show['language'] not in LANGUAGES:
                raise TypeError(f"Language not supported: {show['language']}.")
            # Check for Exact Duplicates using hash of the message dictionary and UUID5 of Rss URL.
            elif previous_record_hash == job_hash:
                raise TypeError(f"File {show['rss_url']} is a duplicate to: {previous_record_hash}.")
            # Same URL Different Body. title says "DELETED"??
            elif previous_record_hash:
                raise QuarantineError({"show_uuid": show['show_uuid'],
                                       "original_show_uuid": show['show_uuid']})
            else:
                self.redis.set(f"{self.entity_type}_{show['show_uuid']}", job_hash)
                self.validate_text_length(show)
                # self.get_field_vectors(show)
                self.get_field_lemmas(show)
                with self.thread_lock:
                    self.complete_queue.put(show)
        except TypeError as err:
            # print(traceback.format_exc())
            self.logger.log_to_purgatory(show, str(err))
        except QuarantineError as quarantine_obj:
            self.logger.log_to_quarantine(quarantine_obj.args[0])
        except ValueError as err:
            self.logger.log_to_purgatory(show, str(err))
        except Exception as err:
            self.logger.log_to_errors(show['show_uuid"'], str(err), traceback.format_exc(), 1)
