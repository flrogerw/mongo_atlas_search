import os
import threading
import queue
import redis
import time
import traceback
from dotenv import load_dotenv
from thread_workers.KafkaProcessor import KafkaProcessor
from sql.PostgresDb import PostgresDb
from datetime import datetime

# Load System ENV VARS
load_dotenv()
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
THREAD_COUNT = int(os.getenv('THREAD_COUNT'))
JOB_RECORDS_TO_PULL = int(os.getenv('JOB_RECORDS_TO_PULL'))
LANGUAGE_MODEL = os.getenv('LANGUAGE_MODEL')
FLUSH_REDIS_ON_START = bool(os.getenv('FLUSH_REDIS_ON_START'))
JOB_QUEUE_SIZE = int(os.getenv('JOB_QUEUE_SIZE'))
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_HOST = os.getenv('DB_HOST')
REDIS_HOST = os.getenv('REDIS_HOST ')

threadLock = threading.Lock()
db = PostgresDb(DB_USER, DB_PASS, DB_DATABASE, DB_HOST)
# Setup Redis
redisCli = redis.Redis(host=REDIS_HOST, port=6379, charset="utf-8", decode_responses=True)
if FLUSH_REDIS_ON_START:
    redisCli.flushdb()

# Set up Queues
errors_q = queue.Queue()
quarantine_q = queue.Queue()
purgatory_q = queue.Queue()


def flush_queues(logger):
    try:
        logger.connect()
        with threadLock:
            purgatory_list = list(purgatory_q.queue)
            purgatory_q.queue.clear()
            errors_list = list(errors_q.queue)
            errors_q.queue.clear()
            quarantine_list = list(quarantine_q.queue)
            quarantine_q.queue.clear()

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
            record_count += (errors_q.qsize() + quarantine_q.qsize() + errors_q.qsize() + purgatory_q.qsize())
            elapsed_time = datetime.now() - start_time
            write_time = datetime.now() - flush_start_time
            print(f'Completed: {record_count} records, Elapsed Time: {elapsed_time} Queue Write: {write_time}')

    except Exception as e:
        print(e)
        with threadLock:
            errors_q.put({"file_name": 'CONSUMER_ERROR', "error": str(e),
                          "stack_trace": traceback.format_exc().replace("\x00", "\uFFFD")})
        pass


if __name__ == '__main__':
    try:
        print('Process Started')
        stop_monitor = False
        threads = []
        for i in range(THREAD_COUNT):
            w = KafkaProcessor(errors_q, quarantine_q, purgatory_q, threadLock, KAFKA_TOPIC)
            # w.daemon = True
            w.start()
            threads.append(w)
        # Start Monitor Thread
        threading.Thread(target=monitor, args=('monitor', lambda: stop_monitor)).start()
        # Wait for threads to finish
        for thread in threads:
            thread.join()

        print('All Threads have Finished')
        # flush_queues(db)
        stop_monitor = True

    except Exception as err:
        print(traceback.format_exc())
        with threadLock:
            errors_q.put({"file_name": 'PIPELINE_ERROR', "error": str(err),
                          "stack_trace": traceback.format_exc().replace("\x00", "\uFFFD")})
            pass
