import os
import uuid
import redis
import queue
import hashlib
import traceback
from datetime import datetime
from iso639 import Lang
from iso639.exceptions import InvalidLanguageValue
from configparser import ConfigParser
from confluent_kafka import Producer, KafkaError, KafkaException
from confluent_avro import AvroKeyValueSerde, SchemaRegistry
from confluent_avro.schema_registry import HTTPBasicAuth
from dotenv import load_dotenv
from sql.PostgresDb import PostgresDb
from fetchers.fetchers import ListenNotesFetcher
from Errors import ValidationError

# Load System ENV VARS
load_dotenv()
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
KAFKA_SCHEMA_REGISTRY_URL = os.getenv('KAFKA_SCHEMA_REGISTRY_URL')
KAFKA_SCHEMA_REGISTRY_KEY = os.getenv('KAFKA_SCHEMA_REGISTRY_KEY')
KAFKA_SCHEMA_REGISTRY_SECRET = os.getenv('KAFKA_SCHEMA_REGISTRY_SECRET')
JOB_RECORDS_TO_PULL = int(os.getenv('JOB_RECORDS_TO_PULL'))
FLUSH_REDIS_ON_START = os.getenv('FLUSH_REDIS_ON_START')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_HOST = os.getenv('DB_HOST')
LISTEN_NOTES_DB_FILE = os.getenv('LISTEN_NOTES_DB_FILE')
LANGUAGES = os.getenv('LANGUAGES').split(",")
UUID_NAMESPACE = os.getenv('UUID_NAMESPACE')

db = PostgresDb(DB_USER, DB_PASS, DB_DATABASE, DB_HOST)
good_record_count = 0
total_record_count = 0
namespace = uuid.uuid5(uuid.NAMESPACE_DNS, UUID_NAMESPACE)
# Setup Redis
redisCli = redis.Redis(host='localhost', port=6379, charset="utf-8", decode_responses=True)
if FLUSH_REDIS_ON_START == 'True':
    redisCli.flushdb()  # Clear hash cache

errors_q = queue.Queue()
purgatory_q = queue.Queue()

config_parser = ConfigParser()
config_parser.read('./kafka-producer.ini')
config = dict(config_parser['local_producer'])
producer = Producer(config)
# Set up Schema Registry
"""
registry_client = SchemaRegistry(
    KAFKA_SCHEMA_REGISTRY_URL,
    HTTPBasicAuth(KAFKA_SCHEMA_REGISTRY_KEY, KAFKA_SCHEMA_REGISTRY_SECRET),
    headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
)
# avro = AvroKeyValueSerde(registry_client, KAFKA_TOPIC)
"""


def log_to_purgatory(message, error_string):
    # print(error_str)
    purgatory_q.put({"podcast_uuid": message['podcast_uuid'],
                     "file_hash": hashlib.md5(message['rss_url'].encode()).hexdigest(),
                     "reason_for_failure": error_string,
                     "rss_url": message['rss_url']})


def log_to_errors(file_name, error_str, stack_trace):
    # print(error_str, stack_trace)
    errors_q.put({"file_name": file_name,
                  "error": error_str,
                  "stack_trace": stack_trace.replace("\x00", "\uFFFD")})


def flush_queues():
    try:
        db.connect()
        purgatory_list = list(purgatory_q.queue)
        errors_list = list(errors_q.queue)
        errors_q.queue.clear()
        purgatory_q.queue.clear()
        if purgatory_list:
            purgatory_inserts = db.append_ingest_ids('purgatory', purgatory_list)
            db.insert_many('purgatory', purgatory_inserts)
        if errors_list:
            db.insert_many('error_log', errors_list)
    except Exception:
        print(traceback.format_exc())
        pass
    finally:
        db.close_connection()


def delivery_report(errmsg, msg):
    if errmsg is not None:
        print("Delivery failed for Message: {} : {}".format(msg.key(), errmsg))
        return
    # print('Message: {} successfully produced to Topic: {} Partition: [{}] at offset {}'.format(
    # msg.key(), msg.topic(), msg.partition(), msg.offset()))


if __name__ == '__main__':
    producer.poll(0)
    fetcher = ListenNotesFetcher(f'../sql/{LISTEN_NOTES_DB_FILE}')
    records = fetcher.fetch('podcasts', JOB_RECORDS_TO_PULL)
    start_time = datetime.now()
    for record in records:
        total_record_count += 1
        try:
            message = {"rss_url": record['feedFilePath'], "language": record['language'],
                       'podcast_uuid': str(uuid.uuid5(namespace, record['feedFilePath']))}
            iso = Lang(message['language'])
            message['language'] = iso.pt1
            if message['language'] not in LANGUAGES:
                raise ValidationError(f"Language not supported: {message['language']}.")
            good_record_count += 1
            kafka_message = str(message).encode()
            producer.produce(topic=KAFKA_TOPIC, key=str(uuid.uuid4()), value=kafka_message, on_delivery=delivery_report)
            producer.poll(0)
            # POST To Kafka
        except InvalidLanguageValue as err:
            # print(traceback.format_exc())
            log_to_purgatory(message, str(err))
        except ValidationError as err:
            # print(traceback.format_exc())
            log_to_purgatory(message, str(err))
        except Exception as err:
            print(traceback.format_exc())
            log_to_errors(record['feedFilePath'], str(err), traceback.format_exc())
        finally:
            end_time = datetime.now() - start_time
            pass
    print(f'Kafka Populated with {good_record_count} messages of {total_record_count} in {end_time}', flush=True)
    print('Flushing Queues', flush=True)
    # flush_queues()
