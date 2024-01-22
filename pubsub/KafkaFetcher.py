import os
import sys
from configparser import ConfigParser
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_avro import AvroKeyValueSerde, SchemaRegistry
from confluent_avro.schema_registry import HTTPBasicAuth
from dotenv import load_dotenv

load_dotenv()
KAFKA_SCHEMA_REGISTRY_URL = os.getenv('KAFKA_SCHEMA_REGISTRY_URL')
KAFKA_SCHEMA_REGISTRY_KEY = os.getenv('KAFKA_SCHEMA_REGISTRY_KEY')
KAFKA_SCHEMA_REGISTRY_SECRET = os.getenv('KAFKA_SCHEMA_REGISTRY_SECRET')


class KafkaFetcher:
    def __init__(self, topic):
        # Set up Consumer
        self.counter = 0
        config_parser = ConfigParser()
        config_parser.read('./pubsub/kafka.ini')
        self.config = dict(config_parser['local_consumer'])
        # Set up Schema Registry
        # registry_client = SchemaRegistry(
        #  KAFKA_SCHEMA_REGISTRY_URL,
        #   HTTPBasicAuth(KAFKA_SCHEMA_REGISTRY_KEY, KAFKA_SCHEMA_REGISTRY_SECRET),
        #  headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        # )
        # self.avro = AvroKeyValueSerde(registry_client, topic)
        self.running = True
        self.topic = topic

    def fetch_one(self):
        try:
            consumer = Consumer(self.config)
            consumer.subscribe([self.topic])

            msg = consumer.poll(timeout=1.0)
            if msg is None:
                return None

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                return msg.value()

        except KafkaException:
            raise

        finally:
            consumer.close()


def fetch_all(self, jobs):
    try:
        consumer = Consumer(self.config)
        consumer.subscribe([self.topic])
        while self.running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                self.running = False
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
