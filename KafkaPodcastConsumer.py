import os
import threading
import queue
import time
import ast
import uuid
import traceback
import sys
from configparser import ConfigParser
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError, TopicPartition
from dotenv import load_dotenv
from thread_workers.PodcastConsumer import PodcastConsumer
from sql.PostgresDb import PostgresDb
from datetime import datetime
from nlp.StanzaNLP import StanzaNLP
from sentence_transformers import SentenceTransformer

# Load System ENV VARS
load_dotenv()
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
KAFKA_EPISODE_TOPIC = os.getenv('KAFKA_EPISODE_TOPIC')
KAFKA_UPLOAD_TOPIC = os.getenv('KAFKA_UPLOAD_TOPIC')
THREAD_COUNT = int(os.getenv('THREAD_COUNT'))
JOB_QUEUE_SIZE = int(os.getenv('JOB_QUEUE_SIZE'))
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_HOST = os.getenv('DB_HOST')
DB_SCHEMA = os.getenv('DB_SCHEMA')
LANGUAGES = os.getenv('LANGUAGES').split(",")
SERVER_CLUSTER_SIZE = int(sys.argv[1])
CLUSTER_SERVER_ID = int(sys.argv[2])
NUMBER_OF_PARTITIONS = int(sys.argv[3])

# Set up Queues
jobs_q = queue.Queue(JOB_QUEUE_SIZE)
quality_q = queue.Queue()
errors_q = queue.Queue()
episodes_q = queue.Queue()
upload_q = queue.Queue()

thread_lock = threading.Lock()
# Load Language Model and Sentence Transformer
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
    except Exception:
        raise

def get_consumer(topic: str = KAFKA_TOPIC) -> Consumer:
    """Initialize and return a Kafka consumer."""
    try:
        config_parser = ConfigParser()
        config_parser.read('./config/kafka.ini')
        config = dict(config_parser['local_consumer'])
        config['group.id'] = 'podcast_consumer'
        kafka_consumer = Consumer(config)
        partitions = get_partitions(topic)
        kafka_consumer.assign(partitions)
        return kafka_consumer
    except Exception:
        raise

def monitor(id: str, stop: callable) -> None:
    """Monitor thread to track processing and handle queue flushing."""
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
    except Exception as err:
        with thread_lock:
            errors_q.put({"entity_identifier": 'PIPELINE_ERROR',
                          "entity_type": 510,
                          "error": str(err),
                          "stack_trace": traceback.format_exc().replace("\x00", "\uFFFD")})
            pass

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
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (event.topic(), event.partition(), event.offset()))
                        pass
                    elif event.error():
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
            pass
