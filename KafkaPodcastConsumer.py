import os
import threading
import queue
import time
import ast
import spacy
import traceback
import sys
from configparser import ConfigParser
from confluent_kafka import Consumer, KafkaException, KafkaError
from dotenv import load_dotenv
from thread_workers.PodcastConsumer import PodcastConsumer
from sql.PostgresDb import PostgresDb
from datetime import datetime
from spacy_langdetect import LanguageDetector
from spacy.language import Language
from sentence_transformers import SentenceTransformer

# Load System ENV VARS
load_dotenv()
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
THREAD_COUNT = int(os.getenv('THREAD_COUNT'))
LANGUAGE_MODEL = os.getenv('LANGUAGE_MODEL')
JOB_QUEUE_SIZE = int(os.getenv('JOB_QUEUE_SIZE'))
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_HOST = os.getenv('DB_HOST')

# Set up Queues
jobs_q = queue.Queue(JOB_QUEUE_SIZE)
quality_q = queue.Queue()
errors_q = queue.Queue()
episodes_q = queue.Queue()

threadLock = threading.Lock()


def get_lang_detector(nlp, name):
    return LanguageDetector()


# Load Language Model and Sentence Transformer
nlp = spacy.load(LANGUAGE_MODEL)
Language.factory("language_detector", func=get_lang_detector)
nlp.add_pipe('language_detector', last=True)
model = SentenceTransformer(os.getenv('VECTOR_MODEL_NAME'))


def get_consumer(topic=KAFKA_TOPIC):
    config_parser = ConfigParser()
    config_parser.read('./config/kafka.ini')
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
            errors_list = list(errors_q.queue)
            errors_q.queue.clear()

        if quality_list:
            quality_inserts = logger.append_ingest_ids('podcast_quality', quality_list)
            logger.insert_many('podcast_quality', quality_inserts)
        if errors_list:
            logger.insert_many('error_log', errors_list)
        logger.close_connection()
    except Exception as e:
        print('DB', print(traceback.format_exc()))
        with threadLock:
            errors_q.put({"identifier": 'PODCAST_CONSUMER_ERROR', "entity_type": "podcast", "error": str(e),
                          "stack_trace": traceback.format_exc().replace("\x00", "\uFFFD")})


def monitor(id, stop):
    try:
        total_completed = 0
        db = PostgresDb(DB_USER, DB_PASS, DB_DATABASE, DB_HOST)
        start_time = datetime.now()
        while True:
            time.sleep(10)
            total_completed += (quality_q.qsize() + errors_q.qsize())
            flush_queues(db)
            if stop():
                break
            elapsed_time = datetime.now() - start_time
            print(f'Completed: {total_completed}  Elapsed Time: {elapsed_time} Jobs Queue Size: {jobs_q.qsize()}')

    except Exception as e:
        print(print(traceback.format_exc()))
        with threadLock:
            errors_q.put({"identifier": 'CONSUMER_ERROR', "error": str(e),
                          "stack_trace": traceback.format_exc().replace("\x00", "\uFFFD")})
        pass


if __name__ == '__main__':
    try:
        print('Kafka Podcast Consumer Started')
        stop_monitor = False
        threads = []
        for i in range(THREAD_COUNT):
            w = PodcastConsumer(jobs_q,
                                quality_q,
                                errors_q,
                                threadLock,
                                nlp,
                                model)
            w.start()
            threads.append(w)

        print('Starting Monitor Thread')
        threading.Thread(target=monitor, args=('monitor', lambda: stop_monitor)).start()

        consumer = get_consumer(KAFKA_TOPIC)
        while True:
            if jobs_q.qsize() < 1000:
                event = consumer.poll(0)
                if event is None:
                    continue
                if event.error():
                    if event.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (event.topic(), event.partition(), event.offset()))
                    elif event.error():
                        raise KafkaException(event.error())
                else:
                    message = event.value().decode('utf8')
                    message = ast.literal_eval(message)
                    jobs_q.put(dict(message))
                    # consumer.commit(event)
            else:
                continue

        jobs_q.join()
        for thread in threads:
            thread.join()

        print('All Threads have Finished')
        stop_monitor = True

    except Exception as err:
        print(traceback.format_exc())
        with threadLock:
            errors_q.put({"identifier": 'PIPELINE_ERROR', "error": str(err),
                          "stack_trace": traceback.format_exc().replace("\x00", "\uFFFD")})
            pass
