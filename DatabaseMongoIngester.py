import os
import threading
import queue
import time
import pymongo
import traceback
from dotenv import load_dotenv
from sql.PostgresDb import PostgresDb
from thread_workers.MongoIngester import MongoIngester
from datetime import datetime
from logger.Logger import ErrorLogger

# Load System ENV VARS
load_dotenv()
THREAD_COUNT = int(os.getenv('SEARCH_THREAD_COUNT'))
JOB_QUEUE_SIZE = int(os.getenv('JOB_QUEUE_SIZE'))
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_HOST = os.getenv('DB_HOST')
DB_SCHEMA = os.getenv('DB_SCHEMA')
SEARCH_FIELDS = os.getenv('PODCAST_SEARCH_FIELDS')
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')
MONGO_USER = os.getenv('MONGO_USER')
MONGO_PASS = os.getenv('MONGO_PASS')
MONGO_HOST = os.getenv('MONGO_HOST')
MONGO_PORT = os.getenv('MONGO_PORT')
LIMIT = 10

thread_lock = threading.Lock()
db = PostgresDb(DB_USER, DB_PASS, DB_DATABASE, DB_HOST, DB_SCHEMA)

mongo_client = pymongo.MongoClient(
    f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/?directConnection=true")
mongo_db = mongo_client[MONGO_DATABASE_NAME]
mongo_collection = mongo_db["podcast_es"]
total = 0

# Set up Queues
errors_q = queue.Queue()
quality_q = queue.Queue()
jobs_q = queue.Queue()

logger = ErrorLogger(thread_lock, errors_q)


def flush_queues(logger):
    try:
        logger.connect()
        with thread_lock:
            search_list = list(quality_q.queue)
            errors_list = list(errors_q.queue)
            errors_q.queue.clear()
            quality_q.queue.clear()
        if errors_list:
            logger.insert_many('error_log', errors_list)
        logger.close_connection()
        mongo_collection.insert_many(search_list)
    except Exception:
        raise


def monitor(id, stop):
    try:
        start_time = datetime.now()
        count = 0
        while True:
            time.sleep(3)
            count += quality_q.qsize()
            elapsed_time = datetime.now() - start_time
            print(f'Insert Queue Size: {quality_q.qsize()} records, Total Records Inserted: {count}, Elapsed Time: {elapsed_time}')
            flush_queues(db)
            if stop():
                break
            else:
                continue
    except Exception as err:
        print(traceback.format_exc())
        logger.log_to_errors('DATABASE_MONGO_INGESTER_ERROR', str(err), traceback.format_exc(), 1)
        pass


if __name__ == '__main__':
    try:
        print('Mongo Ingester Process Started')
        stop_monitor = False
        threads = []
        index_count = 0
        for i in range(THREAD_COUNT):
            w = MongoIngester(jobs_q, quality_q, errors_q, thread_lock)
            w.start()
            threads.append(w)
        print('Starting Monitor Thread')
        threading.Thread(target=monitor, args=('monitor', lambda: stop_monitor)).start()
        language = 'es'
        db.connect()
        batches = db.select_batches('podcast_quality', SEARCH_FIELDS, LIMIT, language)
        for batch in batches:
            jobs_q.put(batch)
            total += len(batch)
        db.close_connection()
        jobs_q.join()
        for thread in threads:
            thread.join()
        print('All Threads have Finished')
        stop_monitor = True

    except Exception as err:
        print(traceback.format_exc())
        logger.log_to_errors('DATABASE_MONGO_INGESTER_ERROR', str(err), traceback.format_exc(), 1)
        pass
