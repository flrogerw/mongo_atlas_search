import threading
import queue
import traceback

from dotenv import load_dotenv
import pickle
from logger.Logger import ErrorLogger
from bson.binary import Binary

# Load System ENV VARS
load_dotenv()


class MongoIngester(threading.Thread):
    def __init__(self,
                 jobs_q,
                 quality_q,
                 errors_q,
                 thread_lock,
                 *args, **kwargs):
        self.logger = ErrorLogger(thread_lock, errors_q)
        self.job_queue = jobs_q
        self.quality_queue = quality_q
        self.thread_lock = thread_lock
        super().__init__(*args, **kwargs)

    def run(self):
        while True:
            try:
                task = self.job_queue.get(timeout=30)
                self.process(task)
            except queue.Empty:
                return

    def process(self, batch):
        for record in batch:
            search_record = {
                "_id": record['podcast_uuid'],
                "podcast_id": record['podcast_quality_id'],
                "is_explicit": record['is_explicit'],
                "publisher": record['publisher'],
                "image_url": record['image_url'],
                "description": record['description_cleaned'],
                "title": record['title_cleaned'],
                "listen_score": record['listen_score_global'],
                "title_lemma": record['title_lemma'],
                "description_lemma": record['description_lemma'],
                "advanced_popularity": record['advanced_popularity'],
                "vector": Binary(record['vector'])
            }
            with self.thread_lock:
                self.quality_queue.put(search_record)
