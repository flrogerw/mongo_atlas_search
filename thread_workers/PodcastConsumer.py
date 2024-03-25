import hashlib
import os
import pickle
import redis
import threading
import queue
import traceback

import requests
from confluent_kafka import KafkaException
from dotenv import load_dotenv
from nlp.Grader import Grader
from logger.Logger import ErrorLogger
from urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

load_dotenv()
# Load System ENV VARS
READABILITY_FIELD = os.getenv('FIELD_FOR_READABILITY')
GET_TOKENS = os.getenv('GET_TOKENS').split(",")
FIELDS_TO_VECTOR = os.getenv('PODCAST_FIELDS_TO_VECTOR').split(",")
REDIS_HOST = os.getenv('REDIS_HOST')
UPLOAD_BUCKET = os.getenv('UPLOAD_BUCKET')
IMAGE_MIME_TYPES = os.getenv('IMAGE_MIME_TYPES').split(",")
DEFAULT_IMAGE_PATH = os.getenv('DEFAULT_IMAGE_PATH')


class PodcastConsumer(threading.Thread):
    def __init__(self, jobs_q, quality_q, errors_q, upload_q, thread_lock, nlp, model, *args, **kwargs):
        self.nlp = nlp
        self.thread_lock = thread_lock
        self.logger = ErrorLogger(thread_lock, errors_q)
        self.model = model
        self.job_queue = jobs_q
        self.upload_q = upload_q
        self.quality_q = quality_q
        self.thread_lock = thread_lock
        self.redis_cli = redis.Redis(host=REDIS_HOST, port=6379, charset="utf-8", decode_responses=True)
        super().__init__(*args, **kwargs)

    def run(self):
        while True:
            try:
                task = self.job_queue.get()
                self.process(task)
            except queue.Empty:
                pass

    def get_field_lemmas(self, message):
        try:
            for key in GET_TOKENS:
                lemma_key = f"{key.split('_')[0]}_lemma"
                message[lemma_key] = self.nlp.get_lemma(message[key], message['language'])
        except Exception:
            raise

    def get_field_vectors(self, kafka_message):
        try:
            for key in FIELDS_TO_VECTOR:
                vector_key = f"{key.split('_')[0]}_vector"
                kafka_message[vector_key] = pickle.dumps(self.nlp.get_vector(kafka_message[key], self.model))
        except Exception:
            raise

    @staticmethod
    def get_path(path_uuid):
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
            with self.thread_lock:
                self.upload_q.put(rss_message)

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
                    with self.thread_lock:
                        self.upload_q.put(image_message)

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

    def get_readability(self, text_str, language='en', grader_type='dale_chall'):
        try:
            grader = Grader(self.nlp.text_processors[language](text_str))
            if grader_type == 'dale_chall':
                return grader.dale_chall_readability_score()
            elif grader_type == 'flesch_kincaid':
                return grader.flesch_kincaid_readability_test()
            elif grader_type == 'gunning_fog':
                return grader.gunning_fog()
            elif grader_type == 'smog_index':
                return grader.smog_index()
            else:
                return 0
        except Exception:
            raise

    def process(self, message):
        try:
            message['image_url'] = self.get_image_path(message['image_url'], message['podcast_uuid'])
            message["rss_url"] = self.get_rss_path(message)
            self.get_field_vectors(message)
            self.get_field_lemmas(message)

            # ADD validation schema HERE

            # Extra Processing goes Here
            if message['language'] == 'en':
                message['readability'] = self.get_readability(message[READABILITY_FIELD])

            with self.thread_lock:
                self.quality_q.put(message)
        except Exception as err:
            self.logger.log_to_errors(message['original_url'], str(err), traceback.format_exc(), 510)
