import os
import sys
from sql.SqliteDb import Db
from configparser import ConfigParser
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_avro import AvroKeyValueSerde, SchemaRegistry
from confluent_avro.schema_registry import HTTPBasicAuth

KAFKA_SCHEMA_REGISTRY_URL = os.getenv('KAFKA_SCHEMA_REGISTRY_URL')
KAFKA_SCHEMA_REGISTRY_KEY = os.getenv('KAFKA_SCHEMA_REGISTRY_KEY')
KAFKA_SCHEMA_REGISTRY_SECRET = os.getenv('KAFKA_SCHEMA_REGISTRY_SECRET')
LISTEN_NOTES_DB_FILE = os.getenv('LISTEN_NOTES_DB_FILE')
JOB_RECORDS_TO_PULL = int(os.getenv('JOB_RECORDS_TO_PULL'))

class KafkaFetcher:
    def __init__(self, topic):
        # Set up Consumer
        self.counter = 0
        config_parser = ConfigParser()
        config_parser.read('./fetchers/kafka-client.ini')
        self.config = dict(config_parser['default'])
        # Set up Schema Registry
        registry_client = SchemaRegistry(
            KAFKA_SCHEMA_REGISTRY_URL,
            HTTPBasicAuth(KAFKA_SCHEMA_REGISTRY_KEY, KAFKA_SCHEMA_REGISTRY_SECRET),
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        )
        self.avro = AvroKeyValueSerde(registry_client, topic)
        self.running = True
        self.topic = topic

    def fetch(self, jobs):
        try:
            consumer = Consumer(self.config)
            consumer.subscribe([self.topic])
            while self.running:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    jobs.put(self.avro.value.deserialize(msg.value()))
                    self.counter += 1
                    if self.counter > JOB_RECORDS_TO_PULL:
                        self.counter = 0
                        self.shutdown()
                        consumer.close()

        except KafkaException:
            raise
        finally:
            # Close down consumer to commit final offsets.
            # consumer.close()
            jobs.join()

    def shutdown(self):
        self.running = False


class ListenNotesFetcher:
    def __init__(self):
        self.db = Db(LISTEN_NOTES_DB_FILE)

    def fetch(self, table_name, limit):
        try:
            offset = 0
            columns = ["rss as feedFilePath"]
            rows = self.db.select_pagination(table_name, columns, limit, offset)
            return rows

        except Exception as err:
            raise
