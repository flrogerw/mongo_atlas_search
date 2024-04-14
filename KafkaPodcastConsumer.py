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


def get_producer():
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


def get_partitions(topic):
    try:
        partition_cluster_index = 0
        partition_clusters = [[] for _ in range(0, SERVER_CLUSTER_SIZE)]
        for partition in range(0, NUMBER_OF_PARTITIONS):
            partition_clusters[partition_cluster_index].append(partition)
            partition_cluster_index = 0 if partition_cluster_index == (
                    SERVER_CLUSTER_SIZE - 1) else partition_cluster_index + 1
        return [TopicPartition(topic, partition_id) for partition_id in partition_clusters[CLUSTER_SERVER_ID]]
    except Exception:
        raise


def drop_on_kafka(self, message, topic):
    kafka_message = str(message).encode()
    self.producer.produce(topic=topic, key=str(uuid.uuid4()), value=kafka_message,
                          on_delivery=self.delivery_report)
    self.producer.flush()


def get_consumer(topic=KAFKA_TOPIC):
    try:
        config_parser = ConfigParser()
        config_parser.read('./config/kafka.ini')
        config = dict(config_parser['local_consumer'])
        config['group.id'] = 'podcast_consumer'
        kafka_consumer = Consumer(config)
        partitions = get_partitions(topic)
        kafka_consumer.assign(partitions)
        # kafka_consumer.subscribe([topic])
        return kafka_consumer
    except Exception:
        raise


def put_episodes(msgs):
    try:
        producer = get_producer()
        producer.begin_transaction()
        producer.poll(0)
        for msg in msgs:
            if int(msg['episode_count']) > 0:
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
            upload_list = list(upload_q.queue)
            upload_q.queue.clear()
            quality_list = list(quality_q.queue)
            quality_q.queue.clear()
            errors_list = list(errors_q.queue)
            errors_q.queue.clear()

        if upload_list:
            producer = get_producer()
            producer.begin_transaction()
            producer.poll(0)
            for message in upload_list:
                kafka_message = str(message).encode()
                producer.produce(topic=KAFKA_UPLOAD_TOPIC, key=str(uuid.uuid4()), value=kafka_message,
                                 on_delivery=delivery_report)
            producer.commit_transaction()
            del producer
        if quality_list:
            quality_inserts = logger.append_ingest_ids('podcast', 'quality', quality_list)
            put_episodes(quality_inserts)
            for qi in quality_inserts: del qi['podcast_uuid'], qi['hash_record'], qi['hash_title'], qi['hash_description']  # Thank Ray for this cluster
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
    except Exception as err:
        # print(traceback.format_exc())
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
            w = PodcastConsumer(jobs_q,
                                quality_q,
                                errors_q,
                                upload_q,
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
        print(traceback.format_exc())
        with thread_lock:
            errors_q.put({"entity_identifier": 'PIPELINE_ERROR',
                          "entity_type": 510,
                          "error": str(err),
                          "stack_trace": traceback.format_exc().replace("\x00", "\uFFFD")})
            pass
