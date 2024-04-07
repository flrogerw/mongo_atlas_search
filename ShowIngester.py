import os
import threading
import queue
import redis
import time
import traceback
from dotenv import load_dotenv
from thread_workers.ShowProcessor import ShowProcessor
from fetchers.SqlLiteFetcher import SqlLiteFetcher
from logger.Logger import ErrorLogger
from sql.PostgresDb import PostgresDb
from datetime import datetime
from nlp.StanzaNLP import StanzaNLP
from sentence_transformers import SentenceTransformer

# Load System ENV VARS
load_dotenv()
THREAD_COUNT = int(os.getenv('THREAD_COUNT'))
JOB_RECORDS_TO_PULL = int(os.getenv('JOB_RECORDS_TO_PULL'))
FLUSH_REDIS_ON_START = os.getenv('FLUSH_REDIS_ON_START')
JOB_QUEUE_SIZE = int(os.getenv('JOB_QUEUE_SIZE'))
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_HOST = os.getenv('DB_HOST')
DB_SCHEMA = os.getenv('DB_SCHEMA')
SHOW_DB_FILE = os.getenv('SHOW_DB_FILE')
REDIS_HOST = os.getenv('REDIS_HOST')
LANGUAGES = os.getenv('LANGUAGES').split(",")

threadLock = threading.Lock()
text_processor = StanzaNLP(LANGUAGES)
record_count = 0
db = PostgresDb(DB_USER, DB_PASS, DB_DATABASE, DB_HOST, DB_SCHEMA)
# Load Language Model and Sentence Transformer
nlp = StanzaNLP(LANGUAGES)
model = SentenceTransformer(os.getenv('VECTOR_MODEL_NAME'))


# Setup Redis
redisCli = redis.Redis(host=REDIS_HOST, port=6379, charset="utf-8", decode_responses=True)
if FLUSH_REDIS_ON_START == 'True':
    redisCli.flushdb()  # Clear hash cache

# Set up Queues
jobs_q = queue.Queue(JOB_QUEUE_SIZE)
quality_q = queue.Queue()
errors_q = queue.Queue()
purgatory_q = queue.Queue()
quarantine_q = queue.Queue()
logger = ErrorLogger(threadLock, errors_q)


def flush_queues(db_logger):
    try:
        db_logger.connect()
        with threadLock:
            quality_list = list(quality_q.queue)
            quality_q.queue.clear()
            purgatory_list = list(purgatory_q.queue)
            purgatory_q.queue.clear()
            errors_list = list(errors_q.queue)
            errors_q.queue.clear()
            quarantine_list = list(quarantine_q.queue)
            quarantine_q.queue.clear()

        if quality_list:
            db_logger.insert_many('show_quality', quality_list)
        if purgatory_list:
            db_logger.insert_many('show_purgatory', purgatory_list)
        if errors_list:
            db_logger.insert_many('error_log', errors_list)
        if quarantine_list:
            db_logger.insert_many('show_quarantine', quarantine_list)
        db_logger.close_connection()
    except Exception as e:
        print(e)
        with threadLock:
            logger.log_to_errors('SHOW_INGESTER', str(err), traceback.format_exc(), 530)
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
                record_count - jobs_q.qsize(), record_count - (record_count - jobs_q.qsize()),
                datetime.now() - start_time, datetime.now() - flush_start_time), flush=True)
    except Exception as e:
        print(e)
        with threadLock:
            errors_q.put({"identifier": 'PIPELINE_ERROR', "error": str(e),
                          "stack_trace": traceback.format_exc().replace("\x00", "\uFFFD")})
        pass


if __name__ == '__main__':
    try:
        print('Show Ingester Process Started')
        stop_monitor = False
        threads = []
        for i in range(THREAD_COUNT):
            w = ShowProcessor(jobs_q, quality_q, errors_q, quarantine_q, purgatory_q, text_processor, threadLock, model)
            # w.daemon = True
            w.start()
            threads.append(w)

        # Start Monitor Thread
        threading.Thread(target=monitor, args=('monitor', lambda: stop_monitor)).start()

        fetcher = SqlLiteFetcher(f'archives/{SHOW_DB_FILE}')
        start, end = fetcher.get_records_offset('show', 1, 0)
        records = fetcher.fetch('show', start, end)
        start_time = datetime.now()
        for record in records:
            record_count += 1
            jobs_q.put(record)
        print(f'Jobs Queue Populated with {record_count} in {datetime.now() - start_time}', flush=True)
        print(f'Records Range:  {start} to {end}', flush=True)
        jobs_q.join()

        for thread in threads:
            thread.join()

        print('All Threads have Finished')
        stop_monitor = True

    except Exception as err:
        print(traceback.format_exc())
        with threadLock:
            print(traceback.format_exc())
            logger.log_to_errors('SHOW_INGESTER', str(err), traceback.format_exc(), 530)
            pass
