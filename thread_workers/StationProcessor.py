from Errors import QuarantineError, ValidationError
import os
import threading
import queue
import redis
import hashlib
import pickle
import traceback
from dotenv import load_dotenv
from nlp.ProcessText import ProcessText
from logger.Logger import ErrorLogger

load_dotenv()

# Load System ENV VARS
GET_TOKENS = os.getenv('GET_TOKENS').split(",")
FIELD_TO_VECTOR = os.getenv('STATION_VECTOR')
MIN_DESCRIPTION_LENGTH = int(os.getenv('MIN_STATION_DESC_LENGTH'))
MIN_TITLE_LENGTH = int(os.getenv('MIN_STATION_TITLE_LENGTH'))
LANGUAGES = os.getenv('LANGUAGES').split(",")
MIN_LANGUAGE_TOLERANCE = os.getenv('MIN_LANGUAGE_TOLERANCE')
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_USER = os.getenv('REDIS_USER')
REDIS_PASS = os.getenv('REDIS_PASS')
STATION_VECTOR = os.getenv('STATION_VECTOR')
READABILITY_FIELD = os.getenv('FIELD_FOR_READABILITY')


class StationProcessor(threading.Thread):
    def __init__(self, job_queue, good_queue, errors_q, quarantine_q, purgatory_q, nlp, model, thread_lock,
                 *args, **kwargs):
        self.nlp = nlp
        self.thread_lock = thread_lock
        self.model = model
        self.logger = ErrorLogger(thread_lock, errors_q, purgatory_q, quarantine_q)
        self.complete_queue = good_queue
        self.job_queue = job_queue
        self.redis = redis.Redis(host=REDIS_HOST, port=6379, charset="utf-8", decode_responses=True)
        self.entity_type = 'station'
        super().__init__(*args, **kwargs)

    def run(self):
        while True:
            try:
                task = self.job_queue.get(timeout=1)
                self.process(task)
            except queue.Empty:
                return
            self.job_queue.task_done()

    @staticmethod
    def construct_description(response, job):
        seo_cleaned = ProcessText.return_clean_text(job['seo_description'])
        response['description_cleaned'] = response['description_cleaned'] if len(response['description_cleaned']) > len(
            seo_cleaned) else seo_cleaned

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

    def get_tokens(self, response, job):
        response['title_lemma'] = ProcessText(f"{job['call_sign']} {response['title_cleaned']}",
                                              response['language']).get_tokens()
        lemma_string = f"{job['slogan']} {response['description_cleaned']}" if job['slogan'] != response[
            'description_cleaned'] else response['description_cleaned']
        response['description_lemma'] = ProcessText(lemma_string, response['language']).get_tokens()

    def process_search_fields(self, msg):
        for key in GET_TOKENS:
            lemma_key = f"{key.split('_')[0]}_lemma"
            msg[lemma_key] = ProcessText.return_lemma(msg[key], msg['language'])
        msg['vector'] = pickle.dumps(ProcessText.get_vector(msg[FIELD_TO_VECTOR], self.model))

    def process(self, job):
        job = dict((k, v if v != '(null)' else '') for k, v in job.items())
        station = {
            "station_uuid": job['station_uuid'],
            "is_explicit": job['is_explicit'],
            "is_searchable": job['is_searchable'],
            "index_status": 310,
            "advanced_popularity": 1,
            "title_cleaned": ProcessText.return_clean_text(job['station_name']),
            "language": job['language'],
            "description_cleaned": ProcessText.return_clean_text(job['description']),
            "image_url": job['image_url']
        }

        try:
            job_hash = hashlib.md5(str(job).encode()).hexdigest()
            previous_station_uuid = self.redis.get(f"{self.entity_type}_{job_hash}")

            if previous_station_uuid == station['station_uuid']:
                raise QuarantineError({"station_uuid": station['station_uuid'],
                                       "original_station_uuid": previous_station_uuid})
            # Set entry in Redis
            else:
                self.redis.set(f"{self.entity_type}_{job_hash}", station['station_uuid'])

            # First Pass at Filtering Garbage
            self.validate_text_length(station)

            # Set some basic values
            if not station['language']:
                station['language'] = ProcessText.get_language_from_model(job['description'], self.nlp)[0]
            if station['language'] not in LANGUAGES:
                raise TypeError(f"Language not supported: {station['language']}.")

            # Do the processing
            self.construct_description(station, job)
            self.validate_text_length(station)
            self.process_search_fields(station)
            self.get_tokens(station, job)
            station['vector'] = pickle.dumps(ProcessText.get_vector(station[STATION_VECTOR], self.model))
            with self.thread_lock:
                self.complete_queue.put(station)

        except TypeError as err:
            # print(traceback.format_exc())
            self.logger.log_to_purgatory(station, str(err))
        except QuarantineError as quarantine_obj:
            self.logger.log_to_quarantine(quarantine_obj)
        except ValidationError as err:
            self.logger.log_to_purgatory(station, str(err.errors()))
        except Exception as err:
            self.logger.log_to_errors(station['station_uuid"'], str(err), traceback.format_exc(), 1)
