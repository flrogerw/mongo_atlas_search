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
# from confluent_avro import AvroKeyValueSerde, SchemaRegistry
# from confluent_avro.schema_registry import HTTPBasicAuth
from dotenv import load_dotenv
from sql.PostgresDb import PostgresDb
from fetchers.ListenNotesFetcher import ListenNotesFetcher
from thread_workers.PodcastProducer import PodcastProducer
from nlp.ProcessText import ProcessText

# Load System ENV VARS
load_dotenv()
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
KAFKA_SCHEMA_REGISTRY_URL = os.getenv('KAFKA_SCHEMA_REGISTRY_URL')
KAFKA_SCHEMA_REGISTRY_KEY = os.getenv('KAFKA_SCHEMA_REGISTRY_KEY')
KAFKA_SCHEMA_REGISTRY_SECRET = os.getenv('KAFKA_SCHEMA_REGISTRY_SECRET')
JOB_RECORDS_TO_PULL = int(os.getenv('JOB_RECORDS_TO_PULL'))
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
NUMBER_OF_PARTITIONS = int(sys.argv[3])

thread_lock = threading.Lock()
text_processor = ProcessText
good_record_count = 0
total_record_count = 0
entity_struct_id = 2

# Set up Queues
jobs_q = queue.Queue(JOB_QUEUE_SIZE)
errors_q = queue.Queue()
purgatory_q = queue.Queue()
quarantine_q = queue.Queue()

db = PostgresDb(DB_USER, DB_PASS, DB_DATABASE, DB_HOST, DB_SCHEMA)

if FLUSH_REDIS_ON_START:
    redis_cli = redis.Redis(host=REDIS_HOST,
                            port=6379,
                            charset="utf-8",
                            decode_responses=True)
    redis_cli.flushdb()  # Clear hash cache

# Set up Schema Registry
"""
registry_client = SchemaRegistry(
    KAFKA_SCHEMA_REGISTRY_URL,
    HTTPBasicAuth(KAFKA_SCHEMA_REGISTRY_KEY, KAFKA_SCHEMA_REGISTRY_SECRET),
    headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
)
# avro = AvroKeyValueSerde(registry_client, KAFKA_TOPIC)
"""


def get_Producer():
    try:
        config_parser = ConfigParser()
        config_parser.read('config/kafka.ini')
        config = dict(config_parser['local_producer'])
        kafka_producer = Producer(config)
        return kafka_producer
    except Exception:
        raise


def flush_queues(logger):
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
            for pi in purgatory_inserts: del pi['podcast_uuid'], pi['record_hash']  # Thank Ray for this cluster
            logger.insert_many('podcast_purgatory', purgatory_inserts)
        if quarantine_list:
            logger.insert_many('podcast_quarantine', quarantine_list)
        if errors_list:
            logger.insert_many('error_log', errors_list)
        logger.close_connection()
    except ValueError as res:
        response, entity_type, table_type = res.args
        inserts = db.error_retry(entity_type, table_type, response)
        if len(inserts) > 0:
            for ins in inserts: del ins['podcast_uuid'], ins['record_hash']  # Thank Ray for this cluster
            logger.insert_many(f"{entity_type}_{table_type}", inserts)
        pass
    except Exception:
        raise


def monitor(x, stop):
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
            errors_q.put({"entity_identifier": 'PIPELINE_ERROR',
                          "entity_type": entity_struct_id,
                          "error": str(e),
                          "stack_trace": traceback.format_exc().replace("\x00", "\uFFFD")})
            pass


if __name__ == '__main__':
    try:
        print('Kafka Podcast Producer Started')
        stop_monitor = False
        threads = []
        for i in range(THREAD_COUNT):
            w = PodcastProducer(jobs_q,
                                purgatory_q,
                                errors_q,
                                quarantine_q,
                                KAFKA_TOPIC,
                                get_Producer(),
                                thread_lock,
                                text_processor)
            threads.append(w)

        fetcher = ListenNotesFetcher(f'archives/{LISTEN_NOTES_DB_FILE}')
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
