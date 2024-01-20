import os
import sys
import threading
import queue
import redis
import time
import spacy
import traceback
from dotenv import load_dotenv
from thread_workers.RssWorker import RssWorker
from sql.PostgresDb import PostgresDb
from fetchers.fetchers import KafkaFetcher, ListenNotesFetcher
from datetime import datetime
from spacy_langdetect import LanguageDetector
from spacy.language import Language
from better_profanity import profanity
from sentence_transformers import SentenceTransformer

# Load System ENV VARS
load_dotenv()
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
THREAD_COUNT = int(os.getenv('THREAD_COUNT'))
JOB_RECORDS_TO_PULL = int(os.getenv('JOB_RECORDS_TO_PULL'))
LANGUAGE_MODEL = os.getenv('LANGUAGE_MODEL')
FLUSH_REDIS_ON_START = os.getenv('FLUSH_REDIS_ON_START')
JOB_QUEUE_SIZE = int(os.getenv('JOB_QUEUE_SIZE'))
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_HOST = os.getenv('DB_HOST')

threadLock = threading.Lock()
db = PostgresDb(DB_USER, DB_PASS, DB_DATABASE, DB_HOST)
record_count = 0

""" Required to load properly below """


def get_lang_detector(nlp, name):
    return LanguageDetector()


# Load Language Model
nlp = spacy.load(LANGUAGE_MODEL)
Language.factory("language_detector", func=get_lang_detector)
nlp.add_pipe('language_detector', last=True)

# Setup Sentence Transformer
model = SentenceTransformer(os.getenv('VECTOR_MODEL_NAME'))

# Setup Redis
redisCli = redis.Redis(host='localhost', port=6379, charset="utf-8", decode_responses=True)
if FLUSH_REDIS_ON_START == 'True':
    redisCli.flushdb()  # Clear hash cache

# Set up Queues
jobs = queue.Queue(JOB_QUEUE_SIZE)
active_q = queue.Queue()
errors_q = queue.Queue()
quarantine_q = queue.Queue()
purgatory_q = queue.Queue()


def flush_queues(logger):
    try:
        logger.connect()
        with threadLock:
            active_list = list(active_q.queue)
            active_q.queue.clear()
            purgatory_list = list(purgatory_q.queue)
            purgatory_q.queue.clear()
            errors_list = list(errors_q.queue)
            errors_q.queue.clear()
            quarantine_list = list(quarantine_q.queue)
            quarantine_q.queue.clear()

        if active_list:
            active_inserts = logger.append_ingest_ids('active', active_list)
            logger.insert_many('active', active_inserts)
        if purgatory_list:
            purgatory_inserts = logger.append_ingest_ids('purgatory', purgatory_list)
            logger.insert_many('purgatory', purgatory_inserts)
        if errors_list:
            logger.insert_many('error_log', errors_list)
        if quarantine_list:
            logger.insert_many('quarantine', quarantine_list)
        logger.close_connection()
    except Exception as e:
        print(e)
        with threadLock:
            errors_q.put({"file_name": 'PIPELINE_ERROR', "error": str(e),
                          "stack_trace": traceback.format_exc().replace("\x00", "\uFFFD")})
        pass


def monitor(id, stop):
    try:
        start_time = datetime.now()
        while True:
            time.sleep(10)
            flush_start_time = datetime.now()
            flush_queues(db)
            if stop():
                break
            print('Completed: {} records, Remaining: {} Total Elapsed Time: {} Queue Write: {}'.format(
                record_count - jobs.qsize(), record_count - (record_count - jobs.qsize()),
                datetime.now() - start_time, datetime.now() - flush_start_time), flush=True)
    except Exception as e:
        print(e)
        with threadLock:
            errors_q.put({"file_name": 'PIPELINE_ERROR', "error": str(e),
                          "stack_trace": traceback.format_exc().replace("\x00", "\uFFFD")})
        pass


if __name__ == '__main__':
    fetcher_type = 'listen_notes'
    try:
        print('Process Started')
        stop_monitor = False
        threads = []
        for i in range(THREAD_COUNT):
            w = RssWorker(jobs, active_q, errors_q, quarantine_q, purgatory_q, nlp, profanity, model,
                          fetcher_type, threadLock)
            # w.daemon = True
            w.start()
            threads.append(w)

        # Start Monitor Thread
        threading.Thread(target=monitor, args=('monitor', lambda: stop_monitor)).start()

        if fetcher_type == 'kafka':
            fetcher = KafkaFetcher(KAFKA_TOPIC)
            fetcher.fetch(jobs)
        elif fetcher_type == 'listen_notes':
            fetcher = ListenNotesFetcher()
            records = fetcher.fetch('podcasts', JOB_RECORDS_TO_PULL)
            with threadLock:
                for record in records:
                    record_count += 1
                    jobs.put(record)
                    if record_count % 10000 == 0:
                        sys.stdout.write("Job Queue Loading: %d   \r" % (record_count))
                        sys.stdout.flush()
            jobs.join()

        for thread in threads:
            thread.join()

        print('All Threads have Finished')
        flush_queues(db)
        stop_monitor = True

    except Exception as err:
        print(traceback.format_exc())
        with threadLock:
            errors_q.put({"file_name": 'PIPELINE_ERROR', "error": str(err),
                          "stack_trace": traceback.format_exc().replace("\x00", "\uFFFD")})
            pass
