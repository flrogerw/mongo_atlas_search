import os
import threading
import queue
import redis
import time
import ast
import spacy
import traceback
from configparser import ConfigParser
from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv
from thread_workers.KafkaProcessor import KafkaProcessor
from sql.PostgresDb import PostgresDb
from datetime import datetime
from spacy_langdetect import LanguageDetector
from spacy.language import Language
from sentence_transformers import SentenceTransformer

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


def get_lang_detector(nlp, name):
    return LanguageDetector()


# Load Language Model
nlp = spacy.load(LANGUAGE_MODEL)
Language.factory("language_detector", func=get_lang_detector)
nlp.add_pipe('language_detector', last=True)

# Setup Sentence Transformer
model = SentenceTransformer(os.getenv('VECTOR_MODEL_NAME'))

# Setup Redis
redisCli = redis.Redis(host='localhost', port=6379, charset="utf-8", decode_responses=True)
if FLUSH_REDIS_ON_START:
    redisCli.flushdb()  # Clear hash cache

# Set up Queues
jobs_q = queue.Queue(JOB_QUEUE_SIZE)
quality_q = queue.Queue()
errors_q = queue.Queue()
quarantine_q = queue.Queue()
purgatory_q = queue.Queue()
episodes_q = queue.Queue()


def get_consumer(topic=KAFKA_TOPIC):
    config_parser = ConfigParser()
    config_parser.read('./pubsub/kafka.ini')
    config = dict(config_parser['local_consumer'])
    kafka_consumer = Consumer(config)
    kafka_consumer.subscribe([topic])
    return kafka_consumer


def flush_queues(logger):
    try:
        logger.connect()
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
            quality_inserts = logger.append_ingest_ids('podcast_quality', quality_list)
            logger.insert_many('podcast_quality', quality_inserts)
        if purgatory_list:
            purgatory_inserts = logger.append_ingest_ids('podcast_purgatory', purgatory_list)
            logger.insert_many('podcast_purgatory', purgatory_inserts)
        if errors_list:
            logger.insert_many('error_log', errors_list)
        if quarantine_list:
            logger.insert_many('podcast_quarantine', quarantine_list)
        logger.close_connection()
    except Exception as e:
        print('DB', print(traceback.format_exc()))
        with threadLock:
            errors_q.put({"identifier": 'CONSUMER_ERROR', "entity_type": "podcast", "error": str(e),
                          "stack_trace": traceback.format_exc().replace("\x00", "\uFFFD")})


def monitor(id, stop):
    record_count = 0
    try:
        start_time = datetime.now()
        while True:
            time.sleep(10)
            record_count += (errors_q.qsize() + quarantine_q.qsize() + quality_q.qsize() + purgatory_q.qsize())
            flush_start_time = datetime.now()
            flush_queues(db)
            if stop():
                break
            elapsed_time = datetime.now() - start_time
            write_time = datetime.now() - flush_start_time
            print(f'Completed: {record_count} records, Elapsed Time: {elapsed_time} Jobs Queue Size: {jobs_q.qsize()}')
        flush_queues(db)

    except Exception as e:
        print( print(traceback.format_exc()))
        with threadLock:
            errors_q.put({"identifier": 'CONSUMER_ERROR', "error": str(e),
                          "stack_trace": traceback.format_exc().replace("\x00", "\uFFFD")})
        pass


if __name__ == '__main__':
    try:
        print('Process Started')
        stop_monitor = False
        threads = []
        for i in range(THREAD_COUNT):
            w = KafkaProcessor(jobs_q, quality_q, errors_q, quarantine_q, purgatory_q, threadLock, nlp, model)
            # w.daemon = True
            w.start()
            threads.append(w)
        # Start Monitor Thread
        print('Starting Monitor Thread')
        threading.Thread(target=monitor, args=('monitor', lambda: stop_monitor)).start()

        consumer = get_consumer(KAFKA_TOPIC)
        while True:
            if jobs_q.qsize() < 1000:
                event = consumer.poll(1.0)
                if event is None:
                    continue
                if event.error():
                    raise KafkaException(event.error())
                else:
                    message = event.value().decode('utf8')
                    message = ast.literal_eval(message)
                    jobs_q.put(dict(message))
                    # consumer.commit(event)
            else:
                continue

        # Wait for threads to finish
        for thread in threads:
            thread.join()

        print('All Threads have Finished')
        # flush_queues(db)
        stop_monitor = True

    except Exception as err:
        print(traceback.format_exc())
        with threadLock:
            errors_q.put({"identifier": 'PIPELINE_ERROR', "error": str(err),
                          "stack_trace": traceback.format_exc().replace("\x00", "\uFFFD")})
            pass
