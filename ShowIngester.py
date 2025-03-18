import os
import threading
import queue
import redis
import time
import traceback
from dotenv import load_dotenv
from datetime import datetime
from thread_workers.ShowProcessor import ShowProcessor
from fetchers.SqlLiteFetcher import SqlLiteFetcher
from logger.Logger import ErrorLogger
from sql.PostgresDb import PostgresDb
from nlp.StanzaNLP import StanzaNLP
from sentence_transformers import SentenceTransformer

# Load System ENV VARS
load_dotenv()

# Global Configuration Variables
THREAD_COUNT: int = int(os.getenv('THREAD_COUNT'))
JOB_RECORDS_TO_PULL: int = int(os.getenv('JOB_RECORDS_TO_PULL'))
FLUSH_REDIS_ON_START: bool = os.getenv('FLUSH_REDIS_ON_START') == 'True'
JOB_QUEUE_SIZE: int = int(os.getenv('JOB_QUEUE_SIZE'))
DB_USER: str = os.getenv('DB_USER')
DB_PASS: str = os.getenv('DB_PASS')
DB_DATABASE: str = os.getenv('DB_DATABASE')
DB_HOST: str = os.getenv('DB_HOST')
DB_SCHEMA: str = os.getenv('DB_SCHEMA')
SHOW_DB_FILE: str = os.getenv('SHOW_DB_FILE')
REDIS_HOST: str = os.getenv('REDIS_HOST')
LANGUAGES: list[str] = os.getenv('LANGUAGES').split(",")

# Thread synchronization lock
thread_lock = threading.Lock()

# Initialize NLP processor and database connection
text_processor = StanzaNLP(LANGUAGES)
db = PostgresDb(DB_USER, DB_PASS, DB_DATABASE, DB_HOST, DB_SCHEMA)
nlp = StanzaNLP(LANGUAGES)
model = SentenceTransformer(os.getenv('VECTOR_MODEL_NAME'))

# Setup Redis Connection
redis_cli = redis.Redis(host=REDIS_HOST, port=6379, charset="utf-8", decode_responses=True)
if FLUSH_REDIS_ON_START:
    redis_cli.flushdb()  # Clear hash cache

# Set up Queues
jobs_q = queue.Queue(JOB_QUEUE_SIZE)
quality_q = queue.Queue()
errors_q = queue.Queue()
purgatory_q = queue.Queue()
quarantine_q = queue.Queue()
logger = ErrorLogger(thread_lock, errors_q)


def flush_queues(db_logger: PostgresDb) -> None:
    """
    Flushes all accumulated records from processing queues into the database.

    Args:
        db_logger (PostgresDb): The database logger instance.
    """
    try:
        db_logger.connect()
        with thread_lock:
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
        print(traceback.format_exc())
        with thread_lock:
            logger.log_to_errors('SHOW_INGESTER', str(e), traceback.format_exc(), 530)


def monitor(monitor_id: str, stop: callable) -> None:
    """
    Monitors the job queue and periodically flushes queued records to the database.

    Args:
        monitor_id (str): Identifier for the monitor instance.
        stop (callable): A function that returns a boolean indicating whether to stop monitoring.
    """
    try:
        start_time = datetime.now()
        while True:
            time.sleep(10)
            flush_start_time = datetime.now()
            flush_queues(db)
            if stop():
                break
            print(f'Completed: {record_count - jobs_q.qsize()} records, Remaining: {jobs_q.qsize()} '
                  f'Total Elapsed Time: {datetime.now() - start_time} '
                  f'Queue Write: {datetime.now() - flush_start_time}', flush=True)
    except Exception as e:
        print(traceback.format_exc())
        with thread_lock:
            errors_q.put({
                "identifier": 'PIPELINE_ERROR',
                "error": str(e),
                "stack_trace": traceback.format_exc().replace("\x00", "\uFFFD")
            })


if __name__ == '__main__':
    try:
        print('Show Ingester Process Started')
        stop_monitor = False
        threads = []

        # Start Worker Threads
        for i in range(THREAD_COUNT):
            processor = ShowProcessor(
                jobs_q, quality_q, errors_q, quarantine_q, purgatory_q, text_processor, thread_lock, model
            )
            processor.start()
            threads.append(processor)

        # Start Monitor Thread
        monitor_thread = threading.Thread(target=monitor, args=('monitor', lambda: stop_monitor))
        monitor_thread.start()

        # Fetch Records
        fetcher = SqlLiteFetcher(f'archives/{SHOW_DB_FILE}')
        start, end = fetcher.get_records_offset('show', 1, 0)
        records = fetcher.fetch('show', start, end)
        start_time = datetime.now()

        for record in records:
            jobs_q.put(record)
        print(f'Jobs Queue Populated with {jobs_q.qsize()} in {datetime.now() - start_time}', flush=True)
        print(f'Records Range: {start} to {end}', flush=True)

        jobs_q.join()

        for thread in threads:
            thread.join()

        print('All Threads have Finished')
        stop_monitor = True

    except Exception as err:
        print(traceback.format_exc())
        with thread_lock:
            logger.log_to_errors('SHOW_INGESTER', str(err), traceback.format_exc(), 530)