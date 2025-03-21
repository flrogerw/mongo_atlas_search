import os
import sys
import queue
import time
import traceback
import redis
import threading
from datetime import datetime
from configparser import ConfigParser
from confluent_kafka import Producer, KafkaError, KafkaException
from dotenv import load_dotenv
from sql.PostgresDb import PostgresDb
from fetchers.SqlLiteFetcher import SqlLiteFetcher
from thread_workers.PodcastProducer import PodcastProducer
from nlp.StanzaNLP import StanzaNLP

# Load System ENV VARS
load_dotenv()
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_HOST = os.getenv('DB_HOST')
DB_SCHEMA = os.getenv('DB_SCHEMA')
LISTEN_NOTES_DB_FILE = os.getenv('LISTEN_NOTES_DB_FILE')
FLUSH_REDIS_ON_START = bool(os.getenv('FLUSH_REDIS_ON_START'))
REDIS_HOST = os.getenv('REDIS_HOST')
THREAD_COUNT = int(os.getenv('THREAD_COUNT'))
JOB_QUEUE_SIZE = int(os.getenv('JOB_QUEUE_SIZE'))
SERVER_CLUSTER_SIZE = int(sys.argv[1])
CLUSTER_SERVER_ID = int(sys.argv[2])
LANGUAGES = os.getenv('LANGUAGES').split(",")

thread_lock = threading.Lock()
text_processor = StanzaNLP(LANGUAGES)
good_record_count = 0
total_record_count = 0
entity_struct_id = 510

# Set up Queues
jobs_q = queue.Queue(JOB_QUEUE_SIZE)
errors_q = queue.Queue()
purgatory_q = queue.Queue()
quarantine_q = queue.Queue()

db = PostgresDb(DB_USER, DB_PASS, DB_DATABASE, DB_HOST, DB_SCHEMA)

if FLUSH_REDIS_ON_START:
    redis_cli = redis.Redis(host=REDIS_HOST, port=6379, charset="utf-8", decode_responses=True)
    redis_cli.flushdb()  # Clear hash cache


def get_producer() -> Producer:
    """
    Initializes and returns a Kafka Producer instance.

    Returns:
        Producer: Kafka Producer instance.
    """
    try:
        config_parser = ConfigParser()
        config_parser.read('config/kafka.ini')
        config = dict(config_parser['local_producer'])
        return Producer(config)
    except Exception as e:
        print(f"Error initializing Kafka Producer: {e}")
        raise


def flush_queues(logger: PostgresDb) -> None:
    """
    Flushes the queued data to the database.

    Args:
        logger (PostgresDb): Database connection object.
    """
    try:
        logger.connect()
        with thread_lock:
            purgatory_list = list(purgatory_q.queue)
            quarantine_list = list(quarantine_q.queue)
            errors_list = list(errors_q.queue)
            purgatory_q.queue.clear()
            errors_q.queue.clear()
            quarantine_q.queue.clear()

        if purgatory_list:
            purgatory_inserts = logger.append_ingest_ids('podcast', 'purgatory', purgatory_list)
            for pi in purgatory_inserts:
                del pi['podcast_uuid'], pi['hash_record'], pi['hash_title'], pi['hash_description']
            logger.insert_many('podcast_purgatory', purgatory_inserts)
        if quarantine_list:
            logger.insert_many('podcast_quarantine', quarantine_list)
        if errors_list:
            logger.insert_many('error_log', errors_list)
        logger.close_connection()
    except Exception as e:
        print(f"Error flushing queues: {e}")
        raise


def monitor(x: str, stop: callable) -> None:
    """
    Monitors the job queue and periodically flushes data to the database.

    Args:
        x (str): Placeholder argument.
        stop (callable): Function to determine when to stop monitoring.
    """
    try:
        starting_time = datetime.now()
        while True:
            time.sleep(10)
            flush_queues(db)
            if stop():
                break
            elapsed_time = datetime.now() - starting_time
            print(f'Elapsed Time: {elapsed_time} Jobs Queue Size: {jobs_q.qsize()}')
    except Exception as e:
        print(traceback.format_exc())
        with thread_lock:
            errors_q.put({
                "entity_identifier": 'PIPELINE_ERROR',
                "entity_type": entity_struct_id,
                "error": str(e),
                "stack_trace": traceback.format_exc().replace("\x00", "\uFFFD")
            })


if __name__ == '__main__':
    try:
        print('Kafka Podcast Producer Started')
        stop_monitor = False
        threads = []
        for i in range(THREAD_COUNT):
            w = PodcastProducer(
                jobs_q,
                purgatory_q,
                errors_q,
                quarantine_q,
                KAFKA_TOPIC,
                get_producer(),
                thread_lock,
                text_processor
            )
            threads.append(w)

        fetcher = SqlLiteFetcher(f'archives/{LISTEN_NOTES_DB_FILE}')
        start, end = fetcher.get_records_offset('podcasts', SERVER_CLUSTER_SIZE, CLUSTER_SERVER_ID)
        records = fetcher.fetch('podcasts', start, end)
        start_time = datetime.now()
        for record in records:
            total_record_count += 1
            jobs_q.put(record)
        print(f'Jobs Queue Populated with {total_record_count} in {datetime.now() - start_time}', flush=True)
        print(f'Records Range:  {start} to {end}', flush=True)

        # Start Monitor Thread
        print('Starting Monitor Thread')
        threading.Thread(target=monitor, args=('place_holder', lambda: stop_monitor)).start()

        print('Starting Worker Threads')
        for thread in threads:
            thread.start()

        jobs_q.join()
        for thread in threads:
            thread.join()

        print('All Threads have Finished')
        stop_monitor = True
    except Exception as err:
        print(traceback.format_exc())