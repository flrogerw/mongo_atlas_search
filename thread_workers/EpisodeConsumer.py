import os
import pickle
import threading
import queue
import traceback
from nlp.ProcessText import ProcessText
from dotenv import load_dotenv
from logger.Logger import ErrorLogger
import requests

load_dotenv()
# Load System ENV VARS
READABILITY_FIELD = os.getenv('FIELD_FOR_READABILITY')
GET_TOKENS = os.getenv('GET_TOKENS').split(",")
FIELD_TO_VECTOR = os.getenv('PODCAST_VECTOR')


class EpisodeConsumer(threading.Thread):
    def __init__(self, jobs_q, quality_q, errors_q, thread_lock, nlp, model, *args, **kwargs):
        self.nlp = nlp
        self.thread_lock = thread_lock
        self.logger = ErrorLogger(thread_lock, errors_q)
        self.model = model
        self.job_queue = jobs_q
        self.quality_q = quality_q
        self.thread_lock = thread_lock
        super().__init__(*args, **kwargs)

    def run(self):
        while True:
            try:
                task = self.job_queue.get(timeout=30)
                self.process(task)
            except queue.Empty:
                return

    @staticmethod
    def get_from_web(file_path):
        try:
            rss = requests.get(file_path, timeout=2.0)
            return rss.text.encode()
        except Exception:
            raise

    def get_search_fields(self, kafka_message):
        for key in GET_TOKENS:
            lemma_key = f"{key.split('_')[0]}_lemma"
            kafka_message[lemma_key] = ProcessText.return_lemma(kafka_message[key], kafka_message['language'])
        kafka_message['vector'] = pickle.dumps(ProcessText.get_vector(kafka_message[FIELD_TO_VECTOR], self.model))

    def process(self, kafka_message):
        try:
            self.get_search_fields(kafka_message)

            with self.thread_lock:
                self.quality_q.put(kafka_message)
        except Exception as err:
            self.logger.log_to_errors(kafka_message['rss_url'], err, traceback.format_exc(), 555)
