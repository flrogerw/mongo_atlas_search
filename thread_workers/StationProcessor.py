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
import time

load_dotenv()

# Load System ENV VARS
GET_TOKENS = os.getenv('GET_TOKENS').split(",")
FIELD_TO_VECTOR = os.getenv('FIELD_TO_VECTOR')
ELEMENTS_TO_PROCESS = os.getenv('ELEMENTS_TO_PROCESS').split(",")
REQUIRED_ELEMENTS = os.getenv('REQUIRED_ELEMENTS').split(",")
MIN_DESC_LENGTH = int(os.getenv('MIN_STATION_DESC_LENGTH'))
MIN_TITLE_LENGTH = int(os.getenv('MIN_STATION_TITLE_LENGTH'))
LANGUAGES = os.getenv('LANGUAGES').split(",")
MIN_LANGUAGE_TOLERANCE = os.getenv('MIN_LANGUAGE_TOLERANCE')
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_USER = os.getenv('REDIS_USER')
REDIS_PASS = os.getenv('REDIS_PASS')
STATION_VECTOR = os.getenv('STATION_VECTOR')


class StationProcessor(threading.Thread):
    def __init__(self, job_queue, good_queue, bad_queue, quarantine_queue, purgatory_queue, nlp, model, thread_lock,
                 *args, **kwargs):
        self.nlp = nlp
        self.thread_lock = thread_lock
        self.model = model
        self.complete_queue = good_queue
        self.purgatory_queue = purgatory_queue
        self.error_queue = bad_queue
        self.quarantine_queue = quarantine_queue
        self.job_queue = job_queue
        self.redis = redis.Redis(host=REDIS_HOST, port=6379, charset="utf-8", decode_responses=True)
        self.redis.select(3)  # Station Cache

        super().__init__(*args, **kwargs)

    def run(self):
        while True:
            try:
                task = self.job_queue.get(timeout=1)
                self.process(task)
            except queue.Empty:
                return
            self.job_queue.task_done()

    def log_to_quarantine(self, station_uuid, matching_uuid, file_name):
        # print(station_uuid, matching_uuid, file_name)
        with self.thread_lock:
            self.quarantine_queue.put({"station_uuid": station_uuid,
                                       "original_station_uuid": matching_uuid,
                                       "duplicate_file_name": file_name})

    def log_to_errors(self, station_uuid, error_str, stack_trace):
        print(stack_trace)
        with self.thread_lock:
            self.error_queue.put({"identifier": station_uuid,
                                  "error": error_str,
                                  "stack_trace": stack_trace.replace("\x00", "\uFFFD")})

    def log_to_purgatory(self, response, error):
        # print(error)
        try:
            log_entry = {
                "station_uuid": response['station_uuid'],
                "language": response['language'],
                "reason_for_failure": error,
                "title": response['title'],
                "description": response['description'],
                "is_searchable": response['is_searchable']
            }
            with self.thread_lock:
                self.purgatory_queue.put(log_entry)
        except Exception:
            raise

    @staticmethod
    def construct_description(response, job):
        response['description'] = response['description'] if len(response['description']) > len(
            job['seo_description']) else job['seo_description']

    @staticmethod
    def validate_text_length(response):
        try:
            description_len = len(response['description'].split(' ')) if response['description'] else 0
            title_len = len(response['title'].split(' ')) if response['title'] else 0
            if description_len < MIN_DESC_LENGTH or title_len < MIN_TITLE_LENGTH:
                raise ValidationError(
                    "Minimum length(s) not met: title {}:{}, description {}:{}."
                    .format(title_len, MIN_TITLE_LENGTH, description_len,
                            MIN_DESC_LENGTH))
        except Exception:
            raise

    def get_tokens(self, response, job):
        response['title_lemma'] = ProcessText(f"{job['call_sign']} {response['title']}",
                                              response['language']).get_tokens()
        lemma_string = f"{job['slogan']} {response['description']}" if job['slogan'] != response['description'] else response['description']
        response['description_lemma'] = ProcessText(lemma_string, response['language']).get_tokens()

    def process_search_fields(self, response):
        for e in ELEMENTS_TO_PROCESS:
            clean_text = ProcessText(response[e], response['language'])
            response[e] = clean_text.get_clean()

    def process(self, job):
        job = dict((k, v if v != '(null)' else '') for k, v in job.items())
        response = {
            "station_uuid": job['station_uuid'], "is_explicit": job['is_explicit'],
            "is_searchable": job['is_searchable'], "index_status": 310, "advanced_popularity": 1,
            "title": job['station_name'], "title_lemma": '-', "language": job['language'],
            "description": job['description'], "description_lemma": '-', "vector": 'n/a', "image_url": job['image_url']
        }

        try:
            job_hash = hashlib.md5(str(job).encode()).hexdigest()
            previous_station_uuid = self.redis.get(job_hash)

            if previous_station_uuid == response['station_uuid']:
                raise QuarantineError(previous_station_uuid)
            # Set entry in Redis
            else:
                self.redis.set(job_hash, response['station_uuid'])

            # First Pass at Filtering Garbage
            self.validate_text_length(response)

            # Set some basic values
            if not response['language']:
                response['language'] = ProcessText.get_language_from_model(job['description'], self.nlp)[0]
            if response['language'] not in LANGUAGES:
                raise ValidationError(f"Language not supported: {response['language']}.")

            # Do the processing
            self.process_search_fields(response)
            self.construct_description(response, job)
            self.get_tokens(response, job)
            self.validate_text_length(response)
            response['vector'] = pickle.dumps(ProcessText.get_vector(response[STATION_VECTOR], self.model))
            with self.thread_lock:
                self.complete_queue.put(response)

        except ValidationError as err:
            self.log_to_purgatory(response, str(err))

        except QuarantineError as previous_station_uuid:
            self.log_to_quarantine(response['station_uuid'], str(previous_station_uuid), job['call_sign'])

        except Exception as err:
            self.log_to_errors(job['station_uuid'], str(err), traceback.format_exc())
