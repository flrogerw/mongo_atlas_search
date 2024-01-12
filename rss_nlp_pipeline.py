import os
import threading
import queue
import redis
import time
import spacy
import traceback
from dotenv import load_dotenv
from thread_workers.RssWorker import RssWorker
from sql.Db import Db
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
db = Db()
record_count = 0

""" Required to load properly below """


def get_lang_detector(nlp, name):
    return LanguageDetector()


# Load Language Model
nlp = spacy.load(os.getenv('LANGUAGE_MODEL'))
Language.factory("language_detector", func=get_lang_detector)
nlp.add_pipe('language_detector', last=True)

# Create, Truncate or do nothing to DB tables
db.create_tables(os.getenv('TRUNCATE_TABLES_ON_START'))

# Setup Sentence Transformer
model = SentenceTransformer(os.getenv('VECTOR_MODEL_NAME'))

# Setup Redis
redisCli = redis.Redis(host='localhost', port=6379, charset="utf-8", decode_responses=True)
if os.getenv('FLUSH_REDIS_ON_START') == 'True':
    redisCli.select(2)
    redisCli.flushdb()  # Clear internal hash cache
    redisCli.select(1)
    redisCli.flushdb()  # Clear etag hash cache

# Set up Queues
jobs = queue.Queue(int(os.getenv('JOB_QUEUE_SIZE')))
good_queue = queue.Queue()
bad_queue = queue.Queue()
quarantine_queue = queue.Queue()
purgatory_queue = queue.Queue()


def flush_queues(logger):
    if not good_queue.empty():
        with threadLock:
            good_list = list(good_queue.queue)
            with good_queue.mutex:
                good_queue.queue.clear()
        logger.insert_many('podcast_active', good_list)

    if not purgatory_queue.empty():
        with threadLock:
            purgatory_list = list(purgatory_queue.queue)
            with purgatory_queue.mutex:
                purgatory_queue.queue.clear()
        logger.insert_many('podcast_purgatory', purgatory_list)

    if not bad_queue.empty():
        with threadLock:
            bad_list = list(bad_queue.queue)
            with bad_queue.mutex:
                bad_queue.queue.clear()
        logger.insert_many('error_log', bad_list)

    if not quarantine_queue.empty():
        with threadLock:
            quarantine_list = list(bad_queue.queue)
            with quarantine_queue.mutex:
                quarantine_queue.queue.clear()
        logger.insert_many('quarantine', quarantine_list)


def monitor(id, stop):
    try:
        logger = Db()
        start_time = datetime.now()
        while True:
            time.sleep(10)
            flush_queues(logger)
            if stop():
                break
            print('Completed: {} records, Remaining: {} Total Elapsed Time: {}'.format(record_count - jobs.qsize(),
                                                                                record_count - (record_count - jobs.qsize()),datetime.now() - start_time),
                  flush=True)
    except Exception:
        raise


if __name__ == '__main__':
    fetcher_type = 'listen_notes'
    try:
        print('Process Started')
        stop_monitor = False
        threads = []
        for i in range(THREAD_COUNT):
            w = RssWorker(jobs, good_queue, bad_queue, quarantine_queue, purgatory_queue, nlp, profanity, model,
                          fetcher_type)
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
        flush_queues(Db())

        stop_monitor = True

    except Exception as err:
        # print(err)
        bad_queue.put({"file_name": 'PIPELINE_ERROR', "error": str(err), "stack_trace": traceback.format_exc()})
        pass
