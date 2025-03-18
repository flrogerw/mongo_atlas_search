import os
import threading
import queue
import time
import ast
import traceback
import sys
import redis
from configparser import ConfigParser
from confluent_kafka import Consumer, KafkaException, KafkaError, TopicPartition
from dotenv import load_dotenv
from thread_workers.FileUploadConsumer import FileUploadConsumer
from sql.PostgresDb import PostgresDb
from datetime import datetime
from typing import List, Callable

# Load System ENV VARS
load_dotenv()
KAFKA_TOPIC: str = os.getenv('KAFKA_UPLOAD_TOPIC', '')
THREAD_COUNT: int = int(os.getenv('THREAD_COUNT', 1))
JOB_QUEUE_SIZE: int = int(os.getenv('JOB_QUEUE_SIZE', 1000))
DB_USER: str = os.getenv('DB_USER', '')
DB_PASS: str = os.getenv('DB_PASS', '')
DB_DATABASE: str = os.getenv('DB_DATABASE', '')
DB_HOST: str = os.getenv('DB_HOST', '')
DB_SCHEMA: str = os.getenv('DB_SCHEMA', '')
FLUSH_REDIS_ON_START: bool = os.getenv('FLUSH_REDIS_ON_START', 'False') == 'True'
REDIS_HOST: str = os.getenv('REDIS_HOST', '')
SERVER_CLUSTER_SIZE: int = int(sys.argv[1])
CLUSTER_SERVER_ID: int = int(sys.argv[2])
NUMBER_OF_PARTITIONS: int = int(sys.argv[3])

# Set up Queues
jobs_q = queue.Queue(JOB_QUEUE_SIZE)
errors_q = queue.Queue()
update_q = queue.Queue()

thread_lock = threading.Lock()

if FLUSH_REDIS_ON_START:
    try:
        redis_cli = redis.Redis(host=REDIS_HOST, port=6379, charset="utf-8", decode_responses=True)
        redis_cli.flushdb()  # Clear hash cache
    except redis.RedisError as e:
        print(f"Redis error: {e}")


def get_partitions(topic: str) -> List[TopicPartition]:
    """
    Get the Kafka partitions assigned to this cluster server.

    Args:
        topic (str): The Kafka topic name.

    Returns:
        List[TopicPartition]: List of assigned topic partitions.
    """
    try:
        partition_cluster_index = 0
        partition_clusters = [[] for _ in range(SERVER_CLUSTER_SIZE)]
        for partition in range(NUMBER_OF_PARTITIONS):
            partition_clusters[partition_cluster_index].append(partition)
            partition_cluster_index = (partition_cluster_index + 1) % SERVER_CLUSTER_SIZE
        return [TopicPartition(topic, pid) for pid in partition_clusters[CLUSTER_SERVER_ID]]
    except Exception as e:
        print(f"Error in get_partitions: {e}")
        raise


def get_consumer(topic: str = KAFKA_TOPIC) -> Consumer:
    """
    Initialize and return a Kafka consumer.

    Args:
        topic (str, optional): The Kafka topic. Defaults to KAFKA_TOPIC.

    Returns:
        Consumer: Configured Kafka consumer.
    """
    try:
        config_parser = ConfigParser()
        config_parser.read('./config/kafka.ini')
        config = dict(config_parser['local_consumer'])
        config['group.id'] = 'upload_consumer'
        kafka_consumer = Consumer(config)
        partitions = get_partitions(topic)
        kafka_consumer.assign(partitions)
        return kafka_consumer
    except Exception as e:
        print(f"Error in get_consumer: {e}")
        raise


def flush_queues(logger: PostgresDb) -> None:
    """
    Flush collected data from queues into the database.

    Args:
        logger (PostgresDb): Database logger instance.
    """
    try:
        logger.connect()
        with thread_lock:
            update_list = list(update_q.queue)
            update_q.queue.clear()
            errors_list = list(errors_q.queue)
            errors_q.queue.clear()

        if update_list:
            logger.insert_many('file_uploads', update_list)
        if errors_list:
            logger.insert_many('error_log', errors_list)
        logger.close_connection()
    except Exception as e:
        print(f"Error in flush_queues: {e}")
        raise


def monitor(id: str, stop: Callable[[], bool]) -> None:
    """
    Monitor and flush queues periodically.

    Args:
        id (str): Identifier for the monitor thread.
        stop (Callable[[], bool]): Function to check if monitoring should stop.
    """
    try:
        total_completed = 0
        db = PostgresDb(DB_USER, DB_PASS, DB_DATABASE, DB_HOST, DB_SCHEMA)
        start_time = datetime.now()
        while True:
            time.sleep(10)
            total_completed += update_q.qsize() + errors_q.qsize()
            flush_queues(db)
            if stop():
                break
            elapsed_time = datetime.now() - start_time
            print(f'Completed: {total_completed} | Elapsed Time: {elapsed_time} | Jobs Queue: {jobs_q.qsize()}')
    except Exception as e:
        print(traceback.format_exc())
        with thread_lock:
            errors_q.put({"entity_identifier": 'UPLOAD PIPELINE_ERROR',
                          "entity_type": 510,
                          "error": str(e),
                          "stack_trace": traceback.format_exc().replace("\x00", "\uFFFD")})


if __name__ == '__main__':
    try:
        print('Kafka File Uploader Consumer Started')
        stop_monitor = False
        threads = []

        for _ in range(THREAD_COUNT):
            worker = FileUploadConsumer(jobs_q, update_q, errors_q, thread_lock)
            worker.start()
            threads.append(worker)

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
                        sys.stderr.write(f'%% {event.topic()} [{event.partition()}] reached end at offset {event.offset()}\n')
                    else:
                        raise KafkaException(event.error())
                else:
                    message = ast.literal_eval(event.value().decode('utf8'))
                    jobs_q.put(dict(message))
            else:
                continue

        jobs_q.join()
        for thread in threads:
            thread.join()
        print('All Threads have Finished')
        stop_monitor = True
    except Exception as err:
        with thread_lock:
            errors_q.put({"entity_identifier": 'FILE UPLOAD PIPELINE_ERROR',
                          "entity_type": 510,
                          "error": str(err),
                          "stack_trace": traceback.format_exc().replace("\x00", "\uFFFD")})