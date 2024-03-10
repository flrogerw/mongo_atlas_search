import os
import threading
import queue
import time
import ast
import uuid
import spacy
import traceback
import sys
from configparser import ConfigParser
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
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
KAFKA_EPISODE_TOPIC = os.getenv('KAFKA_EPISODE_TOPIC')
THREAD_COUNT = int(os.getenv('THREAD_COUNT'))
LANGUAGE_MODEL = os.getenv('LANGUAGE_MODEL')
JOB_QUEUE_SIZE = int(os.getenv('JOB_QUEUE_SIZE'))
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_HOST = os.getenv('DB_HOST')
DB_SCHEMA = os.getenv('DB_SCHEMA')

# Set up Queues
jobs_q = queue.Queue(JOB_QUEUE_SIZE)
quality_q = queue.Queue()
errors_q = queue.Queue()
episodes_q = queue.Queue()

thread_lock = threading.Lock()


def get_lang_detector(nlp, name):
    return LanguageDetector()


# Load Language Model and Sentence Transformer
nlp = spacy.load(LANGUAGE_MODEL)
Language.factory("language_detector", func=get_lang_detector)
nlp.add_pipe('language_detector', last=True)
model = SentenceTransformer(os.getenv('VECTOR_MODEL_NAME'))


def get_Producer():
    try:
        config_parser = ConfigParser()
        config_parser.read('config/kafka.ini')
        config = dict(config_parser['local_producer'])
        config['transactional.id'] = 'poc-transactions'
        kafka_producer = Producer(config)
        kafka_producer.init_transactions()
        return kafka_producer
    except Exception as e:
        print(e)
        raise


def get_consumer(topic=KAFKA_TOPIC):
    config_parser = ConfigParser()
    config_parser.read('./config/kafka.ini')
    config = dict(config_parser['local_consumer'])
    config['group.id'] = 'podcast_consumer'
    kafka_consumer = Consumer(config)
    kafka_consumer.subscribe([topic])
    return kafka_consumer


def put_episodes(msgs):
    try:
        producer = get_Producer()
        producer.begin_transaction()
        #producer.poll(0)
        for msg in msgs:
            if msg['episode_count'] > 0:
                episode_message = {
                    "rss_url": msg['original_url'],
                    "language": msg['language'],
                    "is_explicit": msg['is_explicit'],
                    "podcast_id": msg['podcast_quality_id'],
                    "image_url": msg['image_url'],
                    "publisher": msg['publisher']
                     }
                kafka_message = str(episode_message).encode()
                producer.produce(topic=KAFKA_EPISODE_TOPIC, key=str(uuid.uuid4()), value=kafka_message,
                                 on_delivery=delivery_report)

    except KafkaError:
        raise
    except Exception:
        raise
    finally:
        producer.commit_transaction()


def delivery_report(errmsg, msg):
    if errmsg is not None:
        raise KafkaException(errmsg)


def flush_queues(logger):
    try:
        logger.connect()
        with thread_lock:
            quality_list = list(quality_q.queue)
            quality_q.queue.clear()
            errors_list = list(errors_q.queue)
            errors_q.queue.clear()

        if quality_list:
            quality_inserts = logger.append_ingest_ids('podcast', 'quality', quality_list)
            put_episodes(quality_inserts)
            for qi in quality_inserts: del qi['podcast_uuid'], qi['record_hash']  # Thank Ray for this cluster
            logger.insert_many('podcast_quality', quality_inserts)
        if errors_list:
            logger.insert_many('error_log', errors_list)

    except ValueError as res:
        response, entity_type, table_type = res.args
        inserts = logger.error_retry(entity_type, table_type, response)
        if len(inserts) > 0:
            for ins in inserts: del ins['podcast_uuid'], ins['record_hash']  # Thank Ray for this cluster
            logger.insert_many(f"{entity_type}_{table_type}", inserts)
        pass
    except Exception:
        raise
    finally:
        logger.close_connection()


def monitor(id, stop):
    try:
        total_completed = 0
        db = PostgresDb(DB_USER, DB_PASS, DB_DATABASE, DB_HOST, DB_SCHEMA)
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
        print(traceback.format_exc())
        with thread_lock:
            errors_q.put({"entity_identifier": 'PIPELINE_ERROR',
                          "entity_type": 2,
                          "error": str(e),
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
                                thread_lock,
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
        #print(traceback.format_exc())
        with thread_lock:
            errors_q.put({"entity_identifier": 'PIPELINE_ERROR',
                          "entity_type": 2,
                          "error": err,
                          "stack_trace": traceback.format_exc().replace("\x00", "\uFFFD")})
            pass
