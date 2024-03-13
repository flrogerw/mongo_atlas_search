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

# Load System ENV VARS
load_dotenv()
KAFKA_TOPIC = os.getenv('KAFKA_UPLOAD_TOPIC')
THREAD_COUNT = int(os.getenv('THREAD_COUNT'))
JOB_QUEUE_SIZE = int(os.getenv('JOB_QUEUE_SIZE'))
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_HOST = os.getenv('DB_HOST')
DB_SCHEMA = os.getenv('DB_SCHEMA')
FLUSH_REDIS_ON_START = bool(os.getenv('FLUSH_REDIS_ON_START'))
REDIS_HOST = os.getenv('REDIS_HOST')
SERVER_CLUSTER_SIZE = int(sys.argv[1])
SERVER_ID = int(sys.argv[2])
NUMBER_OF_PARTITIONS = int(sys.argv[3])

# Set up Queues
jobs_q = queue.Queue(JOB_QUEUE_SIZE)
errors_q = queue.Queue()
update_q = queue.Queue()

thread_lock = threading.Lock()
# entity_struct_id = ???

if FLUSH_REDIS_ON_START:
    redis_cli = redis.Redis(host=REDIS_HOST,
                            port=6379,
                            charset="utf-8",
                            decode_responses=True)
    redis_cli.flushdb()  # Clear hash cache


def get_partitions(topic):
    try:
        chunk_size = int(NUMBER_OF_PARTITIONS / SERVER_CLUSTER_SIZE)
        partitions = list(zip(*[iter(range(0, NUMBER_OF_PARTITIONS))] * chunk_size))[SERVER_ID]
        return [TopicPartition(topic, partition) for partition in partitions]
    except Exception:
        raise


def get_consumer(topic=KAFKA_TOPIC):
    try:
        config_parser = ConfigParser()
        config_parser.read('./config/kafka.ini')
        config = dict(config_parser['local_consumer'])
        config['group.id'] = 'image_consumer'
        kafka_consumer = Consumer(config)
        partitions = get_partitions(topic)
        kafka_consumer.assign(partitions)
        # kafka_consumer.subscribe([topic])
        return kafka_consumer
    except Exception:
        raise


def flush_queues(logger):
    try:
        logger.connect()
        with thread_lock:
            update_list = list(update_q.queue)
            update_q.queue.clear()
            errors_list = list(errors_q.queue)
            errors_q.queue.clear()

        if update_list:
            print(update_list)
            # logger.insert_many('episode_quality', quality_list)
        if errors_list:
            logger.insert_many('error_log', errors_list)
        logger.close_connection()
    except Exception:
        raise


def monitor(id, stop):
    try:
        db = PostgresDb(DB_USER, DB_PASS, DB_DATABASE, DB_HOST, DB_SCHEMA)
        start_time = datetime.now()
        while True:
            time.sleep(10)
            flush_queues(db)
            if stop():
                break
            elapsed_time = datetime.now() - start_time
            print(f'Elapsed Time: {elapsed_time} Current Jobs Queue: {jobs_q.qsize()}')
    except Exception as e:
        print(traceback.format_exc())
        with thread_lock:
            errors_q.put({"entity_identifier": 'UPLOAD PIPELINE_ERROR',
                          "entity_type": 1,
                          "error": str(e),
                          "stack_trace": traceback.format_exc().replace("\x00", "\uFFFD")})
            pass


if __name__ == '__main__':
    try:
        print('Kafka File uploader Consumer Started')
        stop_monitor = False
        threads = []
        for i in range(THREAD_COUNT):
            w = FileUploadConsumer(jobs_q,
                                   update_q,
                                   errors_q,
                                   thread_lock)
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
                        pass
                    elif event.error():
                        raise KafkaException(event.error())
                else:
                    message = event.value().decode('utf8')
                    message = ast.literal_eval(message)
                    jobs_q.put(dict(message))
                    # consumer.commit(event)
            else:
                continue

        # The following lines are moot but left for consistency
        jobs_q.join()
        for thread in threads:
            thread.join()
        print('All Threads have Finished')
        stop_monitor = True

    except Exception as err:
        # print(traceback.format_exc())
        with thread_lock:
            errors_q.put({"entity_identifier": 'FILE UPLOAD PIPELINE_ERROR',
                          "entity_type": 1,
                          "error": err,
                          "stack_trace": traceback.format_exc().replace("\x00", "\uFFFD")})
            pass
