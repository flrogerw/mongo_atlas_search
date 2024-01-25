import os
import uuid
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
from fetchers.ListenNotesFetcher import ListenNotesFetcher
from Errors import ValidationError

# Load System ENV VARS
load_dotenv()
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
KAFKA_SCHEMA_REGISTRY_URL = os.getenv('KAFKA_SCHEMA_REGISTRY_URL')
KAFKA_SCHEMA_REGISTRY_KEY = os.getenv('KAFKA_SCHEMA_REGISTRY_KEY')
KAFKA_SCHEMA_REGISTRY_SECRET = os.getenv('KAFKA_SCHEMA_REGISTRY_SECRET')
JOB_RECORDS_TO_PULL = int(os.getenv('JOB_RECORDS_TO_PULL'))
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_HOST = os.getenv('DB_HOST')
LISTEN_NOTES_DB_FILE = os.getenv('LISTEN_NOTES_DB_FILE')
LANGUAGES = os.getenv('LANGUAGES').split(",")
REQUIRED_ELEMENTS = os.getenv('REQUIRED_ELEMENTS').split(",")
UUID_NAMESPACE = os.getenv('UUID_NAMESPACE')
MIN_DESCRIPTION_LENGTH = int(os.getenv('MIN_DESCRIPTION_LENGTH'))
MIN_TITLE_LENGTH = int(os.getenv('MIN_TITLE_LENGTH'))

db = PostgresDb(DB_USER, DB_PASS, DB_DATABASE, DB_HOST)
good_record_count = 0
total_record_count = 0
namespace = uuid.uuid5(uuid.NAMESPACE_DNS, UUID_NAMESPACE)

errors_q = queue.Queue()
purgatory_q = queue.Queue()

config_parser = ConfigParser()
config_parser.read('pubsub/kafka.ini')
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


def log_to_purgatory(mes, error_string):
    # print(error_string)
    purgatory_q.put({"podcast_uuid": mes['podcast_uuid'],
                     "file_hash": hashlib.md5(mes['rss_url'].encode()).hexdigest(),
                     "reason_for_failure": error_string,
                     "rss_url": mes['rss_url']})


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


def validate_requirements(rec):
    try:
        # Make sure all required elements are present
        for element in REQUIRED_ELEMENTS:
            if element not in rec:
                raise ValidationError(f"Record is missing required element: {element}.")

        description_len = len(rec['description'].split(' '))
        title_len = len(rec['title'].split(' '))
        if description_len < MIN_DESCRIPTION_LENGTH or title_len < MIN_TITLE_LENGTH:
            raise ValidationError(
                f"Minimum length(s) not met: title {title_len}:{MIN_TITLE_LENGTH}, description {description_len}:{MIN_DESCRIPTION_LENGTH}.")
    except Exception:
        raise


def delivery_report(errmsg, msg):
    if errmsg is not None:
        print(f"Delivery failed for Message: {msg.key()} : {errmsg}")
        return
    print('Message: {} successfully produced to Topic: {} Partition: [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


if __name__ == '__main__':
    producer.poll(0)
    fetcher = ListenNotesFetcher(f'sql/{LISTEN_NOTES_DB_FILE}')
    records = fetcher.fetch('podcasts', JOB_RECORDS_TO_PULL)
    start_time = datetime.now()
    for record in records:
        total_record_count += 1
        try:
            message = {"rss_url": record['rss'], "language": record['language'],
                       'podcast_uuid': str(uuid.uuid5(namespace, record['rss']))}
            iso = Lang(message['language'])
            message['language'] = iso.pt1
            if message['language'] not in LANGUAGES:
                raise ValidationError(f"Language not supported: {message['language']}.")
            validate_requirements(record)
            good_record_count += 1
            kafka_message = str(message).encode()
            #producer.produce(topic=KAFKA_TOPIC, key=str(uuid.uuid4()), value=kafka_message, on_delivery=delivery_report)
            #producer.poll(0)
        except InvalidLanguageValue as err:
            # print(traceback.format_exc())
            log_to_purgatory(message, str(err))
        except ValidationError as err:
            # print(traceback.format_exc())
            log_to_purgatory(message, str(err))
        except Exception as err:
            print(traceback.format_exc())
            log_to_errors(record['rss'], str(err), traceback.format_exc())
        finally:
            end_time = datetime.now() - start_time
            pass
    print(f'Kafka Populated with {good_record_count} messages of {total_record_count} in {end_time}', flush=True)
    print('Flushing Queues', flush=True)
    # flush_queues()
