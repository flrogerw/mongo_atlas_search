import os
import threading
import queue
import time
import traceback
from dotenv import load_dotenv
from sql.PostgresDb import PostgresDb
from open_search.SendToSearch import SendToSearch
from datetime import datetime

# Load system environment variables
load_dotenv()
THREAD_COUNT = int(os.getenv('SEARCH_THREAD_COUNT', 4))  # Default to 4 if not set
JOB_QUEUE_SIZE = int(os.getenv('JOB_QUEUE_SIZE', 100))  # Default queue size
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_HOST = os.getenv('DB_HOST')
DB_SCHEMA = os.getenv('DB_SCHEMA')
SEARCH_FIELDS = os.getenv('PODCAST_SEARCH_FIELDS', '')
LIMIT = 500  # Number of records to fetch at a time

# Global thread lock
thread_lock = threading.Lock()

# Queues for managing jobs and errors
errors_q = queue.Queue()
jobs_q = queue.Queue()

# Global variable to track the total number of processed records
total = 0


def flush_queues(logger: PostgresDb) -> None:
    """
    Flushes error messages from the queue into the database error log.

    Args:
        logger (PostgresDb): The database connection for logging errors.

    Raises:
        Exception: Logs any errors that occur during flushing.
    """
    try:
        logger.connect()
        with thread_lock:
            errors_list = list(errors_q.queue)
            errors_q.queue.clear()

        if errors_list:
            logger.insert_many('error_log', errors_list)
        logger.close_connection()
    except Exception as e:
        print(f"Error while flushing queues: {e}")
        with thread_lock:
            errors_q.put({
                "file_name": 'CONSUMER_ERROR',
                "error": str(e),
                "stack_trace": traceback.format_exc().replace("\x00", "\uFFFD")
            })


def monitor(thread_id: str, stop_condition) -> None:
    """
    Monitors the job queue and logs queue size and inserted records.

    Args:
        thread_id (str): Identifier for the monitoring thread.
        stop_condition (Callable): Function that determines when to stop monitoring.
    """
    try:
        start_time = datetime.now()
        while True:
            time.sleep(10)  # Check queue status every 10 seconds
            flush_start_time = datetime.now()
            # flush_queues(db)  # Uncomment if needed
            if stop_condition():
                break

            elapsed_time = datetime.now() - start_time
            write_time = datetime.now() - flush_start_time
            print(f'Queue Size: {jobs_q.qsize()} records, Records Inserted: {total}, Elapsed Time: {elapsed_time}')
    except Exception as e:
        print(f"Monitoring error: {e}")
        with thread_lock:
            errors_q.put({
                "file_name": 'CONSUMER_ERROR',
                "error": str(e),
                "stack_trace": traceback.format_exc().replace("\x00", "\uFFFD")
            })


if __name__ == '__main__':
    try:
        print('Process Started')
        stop_monitor = False
        threads = []
        index_count = 0

        # Create worker threads
        for i in range(THREAD_COUNT):
            index = f"podcast_en_{index_count}"
            index_count = (index_count + 1) if index_count < 3 else 0
            worker = SendToSearch(index, jobs_q, errors_q, thread_lock)
            # worker.daemon = True  # Uncomment if needed
            worker.start()
            threads.append(worker)

        # Start Monitor Thread
        print('Starting Monitor Thread')
        threading.Thread(target=monitor, args=('monitor', lambda: stop_monitor)).start()

        # Initialize database connection
        db = PostgresDb(DB_USER, DB_PASS, DB_DATABASE, DB_HOST, DB_SCHEMA)
        db.connect()
        offset = 1
        language = 'en'

        while True:
            try:
                # Fetch search fields data from the database
                docs = db.select_search_fields('podcast_quality', SEARCH_FIELDS, language, offset, LIMIT)
                if not docs:
                    break

                # jobs_q.put(docs)  # Uncomment if you want to enqueue documents
                total += len(docs)
                offset = total + 1
                continue
            except Exception as e:
                print(f"Data fetch error: {e}")
                break

        jobs_q.join()
        print('All jobs processed')

        # Wait for worker threads to finish
        for thread in threads:
            thread.join()

        print('All Threads have Finished')
        # flush_queues(db)  # Uncomment if needed
        stop_monitor = True

    except Exception as err:
        print(traceback.format_exc())
        with thread_lock:
            errors_q.put({
                "file_name": 'PIPELINE_ERROR',
                "error": str(err),
                "stack_trace": traceback.format_exc().replace("\x00", "\uFFFD")
            })
