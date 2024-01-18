import os
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
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
THREAD_COUNT = int(os.getenv('THREAD_COUNT'))
JOB_RECORDS_TO_PULL = int(os.getenv('JOB_RECORDS_TO_PULL'))

load_dotenv()
threadLock = threading.Lock()
"""
db = PostgresDb('podcast_rw',
                'write',
                'pod_manager',
                'podcast.cxrfoquw9xex.us-east-1.rds.amazonaws.com'
                )
"""
db = PostgresDb('postgres',
                'postgres',
                'postgres',
                'localhost'
                )

record_count = 0

""" Required to load properly below """


def get_lang_detector(nlp, name):
    return LanguageDetector()


# Load Language Model
nlp = spacy.load(os.getenv('LANGUAGE_MODEL'))
Language.factory("language_detector", func=get_lang_detector)
nlp.add_pipe('language_detector', last=True)

# Create, Truncate or do nothing to DB tables
# db.create_tables(os.getenv('TRUNCATE_TABLES_ON_START'))

# Setup Sentence Transformer
model = SentenceTransformer(os.getenv('VECTOR_MODEL_NAME'))

# Setup Redis
redisCli = redis.Redis(host='localhost', port=6379, charset="utf-8", decode_responses=True)
if os.getenv('FLUSH_REDIS_ON_START') == 'True':
    redisCli.flushdb()  # Clear etag hash cache

# Set up Queues
jobs = queue.Queue(int(os.getenv('JOB_QUEUE_SIZE')))
active_q = queue.Queue()
error_q = queue.Queue()
quarantine_q = queue.Queue()
purgatory_q = queue.Queue()


def flush_queues(logger):
    try:
        logger.connect()
        with threadLock:
            active = list(active_q.queue)
            purgatory = list(purgatory_q.queue)
            error = list(error_q.queue)
            quarantine = list(quarantine_q.queue)

            active_q.queue.clear()
            purgatory_q.queue.clear()
            error_q.queue.clear()
            quarantine_q.queue.clear()

        if active:
            inserts = logger.append_ingest_ids('active', active)
            logger.insert_many('active', inserts)
        if purgatory:
            inserts = logger.append_ingest_ids('purgatory', purgatory)
            #logger.insert_many('purgatory', inserts)
        if error:
            x=0
            # logger.insert_many('error_log', error)
        if quarantine:
            inserts = logger.append_ingest_ids('quarantine', quarantine)
            logger.insert_many('quarantine', inserts)
        logger.close_connection()
    except Exception as err:
        print(err)
        raise


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
    except Exception as err:
        with threadLock:
            error_q.put({"file_name": 'PIPELINE_ERROR', "error": str(err), "stack_trace": traceback.format_exc()})


if __name__ == '__main__':
    fetcher_type = 'listen_notes'
    try:
        print('Process Started')
        stop_monitor = False
        threads = []
        for i in range(THREAD_COUNT):
            w = RssWorker(jobs, active_q, error_q, quarantine_q, purgatory_q, nlp, profanity, model,
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
            for record in records:
                record_count += 1
                jobs.put(record)
            jobs.join()

        for thread in threads:
            thread.join()

        print('All Threads have Finished')
        flush_queues(db)
        stop_monitor = True

    except Exception as err:
        print(traceback.format_exc())
        with threadLock:
            error_q.put({"file_name": 'PIPELINE_ERROR', "error": str(err), "stack_trace": traceback.format_exc()})
            pass
