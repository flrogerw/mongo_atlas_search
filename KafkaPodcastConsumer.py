import os
import threading
import queue
import time
import ast
import uuid
import traceback
import sys
from configparser import ConfigParser
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
from dotenv import load_dotenv
from thread_workers.PodcastConsumer import PodcastConsumer
from sql.PostgresDb import PostgresDb
from datetime import datetime
from nlp.StanzaNLP import StanzaNLP
from sentence_transformers import SentenceTransformer

# Load system environment variables
load_dotenv()
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
THREAD_COUNT = int(os.getenv('THREAD_COUNT'))
JOB_QUEUE_SIZE = int(os.getenv('JOB_QUEUE_SIZE'))
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_HOST = os.getenv('DB_HOST')
DB_SCHEMA = os.getenv('DB_SCHEMA')
LANGUAGES = os.getenv('LANGUAGES').split(",")

# Set up queues
jobs_q: queue.Queue = queue.Queue(JOB_QUEUE_SIZE)
quality_q: queue.Queue = queue.Queue()
errors_q: queue.Queue = queue.Queue()
upload_q: queue.Queue = queue.Queue()

thread_lock = threading.Lock()

# Load NLP model and sentence transformer
nlp = StanzaNLP(LANGUAGES)
model = SentenceTransformer(os.getenv('VECTOR_MODEL_NAME'))


def get_producer() -> Producer:
    """Initialize and return a Kafka producer."""
    try:
        config_parser = ConfigParser()
        config_parser.read('config/kafka.ini')
        config = dict(config_parser['local_producer'])
        config['transactional.id'] = str(uuid.uuid4())
        kafka_producer = Producer(config)
        kafka_producer.init_transactions()
        return kafka_producer
    except Exception as e:
        print("Error initializing Kafka producer:", e)
        raise


def get_consumer(topic: str = KAFKA_TOPIC) -> Consumer:
    """Initialize and return a Kafka consumer.

    Args:
        topic (str): The Kafka topic to consume from.

    Returns:
        Consumer: Configured Kafka consumer instance.
    """
    try:
        config_parser = ConfigParser()
        config_parser.read('./config/kafka.ini')
        config = dict(config_parser['local_consumer'])
        config['group.id'] = 'podcast_consumer'
        kafka_consumer = Consumer(config)
        return kafka_consumer
    except Exception as e:
        print("Error initializing Kafka consumer:", e)
        raise


def monitor(id: str, stop: callable) -> None:
    """Monitor thread to track processing and handle queue flushing.

    Args:
        id (str): Monitor thread identifier.
        stop (callable): Function to check if monitoring should stop.
    """
    try:
        total_completed = 0
        db = PostgresDb(DB_USER, DB_PASS, DB_DATABASE, DB_HOST, DB_SCHEMA)
        start_time = datetime.now()
        while True:
            time.sleep(10)
            total_completed += (quality_q.qsize() + errors_q.qsize())
            if stop():
                break
            elapsed_time = datetime.now() - start_time
            print(f'Completed: {total_completed}  Elapsed Time: {elapsed_time} Jobs Queue Size: {jobs_q.qsize()}')
    except Exception as err:
        with thread_lock:
            errors_q.put({"entity_identifier": 'PIPELINE_ERROR',
                          "entity_type": 510,
                          "error": str(err),
                          "stack_trace": traceback.format_exc().replace("\x00", "\uFFFD")})


if __name__ == '__main__':
    try:
        print('Kafka Podcast Consumer Started')
        stop_monitor = False
        threads = []

        for i in range(THREAD_COUNT):
            w = PodcastConsumer(jobs_q, quality_q, errors_q, upload_q, thread_lock, nlp, model)
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
                        sys.stderr.write(
                            f'%% {event.topic()} [{event.partition()}] reached end at offset {event.offset()}\n')
                        continue
                    else:
                        raise KafkaException(event.error())
                else:
                    message = event.value().decode('utf8')
                    message = ast.literal_eval(message)
                    jobs_q.put(dict(message))
            else:
                continue

        jobs_q.join()
        for thread in threads:
            thread.join()
        print('All Threads have Finished')
        stop_monitor = True

    except Exception as err:
        print(traceback.format_exc())
        with thread_lock:
            errors_q.put({"entity_identifier": 'PIPELINE_ERROR',
                          "entity_type": 510,
                          "error": str(err),
                          "stack_trace": traceback.format_exc().replace("\x00", "\uFFFD")})
