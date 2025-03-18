import os
import threading
import queue
import time
import ast
import spacy
import traceback
import sys
import redis
from configparser import ConfigParser
from confluent_kafka import Consumer, KafkaException, KafkaError, TopicPartition
from dotenv import load_dotenv
from thread_workers.EpisodeConsumer import EpisodeConsumer
from sql.PostgresDb import PostgresDb
from datetime import datetime
from spacy_langdetect import LanguageDetector
from spacy.language import Language
from sentence_transformers import SentenceTransformer
from nlp.StanzaNLP import StanzaNLP
from typing import List, Dict, Any, Callable

# Load System ENV VARS
load_dotenv()
KAFKA_TOPIC: str = os.getenv('KAFKA_EPISODE_TOPIC', '')
THREAD_COUNT: int = int(os.getenv('THREAD_COUNT', '1'))
LANGUAGES: List[str] = os.getenv('LANGUAGES', '').split(',')
JOB_QUEUE_SIZE: int = int(os.getenv('JOB_QUEUE_SIZE', '100'))
DB_USER: str = os.getenv('DB_USER', '')
DB_PASS: str = os.getenv('DB_PASS', '')
DB_DATABASE: str = os.getenv('DB_DATABASE', '')
DB_HOST: str = os.getenv('DB_HOST', '')
DB_SCHEMA: str = os.getenv('DB_SCHEMA', '')
FLUSH_REDIS_ON_START: bool = bool(os.getenv('FLUSH_REDIS_ON_START', 'False'))
REDIS_HOST: str = os.getenv('REDIS_HOST', '')
SERVER_CLUSTER_SIZE: int = int(sys.argv[1])
CLUSTER_SERVER_ID: int = int(sys.argv[2])
NUMBER_OF_PARTITIONS: int = int(sys.argv[3])

# Set up Queues
jobs_q: queue.Queue = queue.Queue(JOB_QUEUE_SIZE)
quality_q: queue.Queue = queue.Queue()
errors_q: queue.Queue = queue.Queue()
episodes_q: queue.Queue = queue.Queue()
purgatory_q: queue.Queue = queue.Queue()
quarantine_q: queue.Queue = queue.Queue()

thread_lock = threading.Lock()
entity_struct_id: int = 520

if FLUSH_REDIS_ON_START:
    redis_cli = redis.Redis(host=REDIS_HOST, port=6379, charset='utf-8', decode_responses=True)
    redis_cli.flushdb()  # Clear hash cache


def get_lang_detector(nlp: Language, name: str) -> LanguageDetector:
    """
    Returns a language detector instance for Spacy NLP.

    Args:
        nlp (Language): Spacy NLP pipeline.
        name (str): Name of the component.

    Returns:
        LanguageDetector: Language detection component.
    """
    return LanguageDetector()


# Load NLP Model and Sentence Transformer
nlp = StanzaNLP(LANGUAGES)
model = SentenceTransformer(os.getenv('VECTOR_MODEL_NAME', ''))


def get_partitions(topic: str) -> List[TopicPartition]:
    """
    Generates a list of topic partitions assigned to the server cluster.

    Args:
        topic (str): Kafka topic name.

    Returns:
        List[TopicPartition]: List of topic partitions assigned to the server cluster.
    """
    try:
        partition_clusters = [[] for _ in range(SERVER_CLUSTER_SIZE)]
        partition_cluster_index = 0
        for partition in range(NUMBER_OF_PARTITIONS):
            partition_clusters[partition_cluster_index].append(partition)
            partition_cluster_index = (partition_cluster_index + 1) % SERVER_CLUSTER_SIZE
        return [TopicPartition(topic, pid) for pid in partition_clusters[CLUSTER_SERVER_ID]]
    except Exception as e:
        raise RuntimeError("Error getting partitions") from e


def get_consumer(topic: str = KAFKA_TOPIC) -> Consumer:
    """
    Creates and returns a Kafka consumer instance.

    Args:
        topic (str): Kafka topic name.

    Returns:
        Consumer: Kafka consumer instance.
    """
    try:
        config_parser = ConfigParser()
        config_parser.read('./config/kafka.ini')
        config = dict(config_parser['local_consumer'])
        config['group.id'] = 'episode_consumer'
        kafka_consumer = Consumer(config)
        partitions = get_partitions(topic)
        kafka_consumer.assign(partitions)
        return kafka_consumer
    except Exception as e:
        raise RuntimeError("Error creating Kafka consumer") from e


def monitor(id: str, stop: Callable[[], bool]) -> None:
    """
    Monitors the job queue and periodically flushes data to the database.

    Args:
        id (str): Identifier for the monitor.
        stop (Callable[[], bool]): Function to determine when to stop monitoring.
    """
    try:
        total_completed = 0
        db = PostgresDb(DB_USER, DB_PASS, DB_DATABASE, DB_HOST, DB_SCHEMA)
        start_time = datetime.now()
        while not stop():
            time.sleep(10)
            total_completed += (quality_q.qsize() + purgatory_q.qsize() + errors_q.qsize())
            flush_queues(db)
            elapsed_time = datetime.now() - start_time
            print(f'Completed: {total_completed} Elapsed Time: {elapsed_time} Jobs Queue Size: {jobs_q.qsize()}')
    except Exception as e:
        print(traceback.format_exc())


if __name__ == '__main__':
    try:
        print('Kafka Episode Consumer Started')
        stop_monitor = False
        threads = []
        for i in range(THREAD_COUNT):
            worker = EpisodeConsumer(jobs_q, quality_q, errors_q, purgatory_q, quarantine_q, thread_lock, nlp, model)
            worker.start()
            threads.append(worker)

        print('Starting Monitor Thread')
        monitor_thread = threading.Thread(target=monitor, args=('monitor', lambda: stop_monitor))
        monitor_thread.start()

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
                    else:
                        raise KafkaException(event.error())
                else:
                    message = event.value().decode('utf8')
                    jobs_q.put(ast.literal_eval(message))
            else:
                continue

        jobs_q.join()
        for thread in threads:
            thread.join()

        print('All Threads have Finished')
        stop_monitor = True

    except Exception as err:
        with thread_lock:
            errors_q.put({"entity_identifier": 'PIPELINE_ERROR', "entity_type": 520, "error": str(err),
                          "stack_trace": traceback.format_exc()})