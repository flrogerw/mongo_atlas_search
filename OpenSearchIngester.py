import os
import threading
import queue
import time
import traceback
from dotenv import load_dotenv
from sql.PostgresDb import PostgresDb
from search.SendToSearch import SendToSearch
from datetime import datetime

# Load System ENV VARS
load_dotenv()
THREAD_COUNT = int(os.getenv('SEARCH_THREAD_COUNT'))
JOB_QUEUE_SIZE = int(os.getenv('JOB_QUEUE_SIZE'))
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_HOST = os.getenv('DB_HOST')
SEARCH_FIELDS = os.getenv('SEARCH_FIELDS')
LIMIT=500

thread_lock = threading.Lock()
db = PostgresDb(DB_USER, DB_PASS, DB_DATABASE, DB_HOST)
total = 0

# Set up Queues
errors_q = queue.Queue()
jobs_q = queue.Queue()


def flush_queues(logger):
    try:
        logger.connect()
        with thread_lock:
            errors_list = list(errors_q.queue)
            errors_q.queue.clear()

        if errors_list:
            logger.insert_many('error_log', errors_list)
        logger.close_connection()
    except Exception as e:
        print(e)
        with thread_lock:
            errors_q.put({"file_name": 'CONSUMER_ERROR', "error": str(e),
                          "stack_trace": traceback.format_exc().replace("\x00", "\uFFFD")})
        pass


def monitor(id, stop):
    record_count = 0
    try:
        start_time = datetime.now()
        while True:
            time.sleep(10)
            flush_start_time = datetime.now()
            # flush_queues(db)
            if stop():
                break
            record_count += 0
            elapsed_time = datetime.now() - start_time
            write_time = datetime.now() - flush_start_time
            print(f'Queue Size: {jobs_q.qsize()} records, Records Inserted: {total}, Elapsed Time: {elapsed_time}')

    except Exception as e:
        print(e)
        with thread_lock:
            errors_q.put({"file_name": 'CONSUMER_ERROR', "error": str(e),
                          "stack_trace": traceback.format_exc().replace("\x00", "\uFFFD")})
        pass


if __name__ == '__main__':
    try:
        print('Process Started')
        stop_monitor = False
        threads = []
        for i in range(THREAD_COUNT):
            if i % 2 == 0:
                index = 'podcast_en_0'
            else:
                index = 'podcast_en_0'
            w = SendToSearch(index, jobs_q, errors_q, thread_lock)
            # w.daemon = True
            w.start()
            threads.append(w)
        # Start Monitor Thread
        print('Starting Monitor Thread')
        threading.Thread(target=monitor, args=('monitor', lambda: stop_monitor)).start()

        db = PostgresDb(DB_USER, DB_PASS, DB_DATABASE, DB_HOST)
        db.connect()
        offset = 1
        limit = LIMIT
        language = 'es'

        while True:
            try:
                docs = db.select_search_fields('active', SEARCH_FIELDS, language, offset, limit)
                if not docs:
                    break
                jobs_q.put(docs)
                total += len(docs)
                offset = total + 1
                continue
            except Exception:
                break
        jobs_q.join()
        print('broken out')
        # Wait for threads to finish
        for thread in threads:
            thread.join()

        print('All Threads have Finished')
        # flush_queues(db)
        stop_monitor = True

    except Exception as err:
        print(traceback.format_exc())
        with thread_lock:
            errors_q.put({"file_name": 'PIPELINE_ERROR', "error": str(err),
                          "stack_trace": traceback.format_exc().replace("\x00", "\uFFFD")})
            pass
