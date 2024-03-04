import os
import pickle
import uuid
import threading
import queue
import traceback
from typing import Optional

from nlp.ProcessText import ProcessText
from dotenv import load_dotenv
from logger.Logger import ErrorLogger
from Errors import ValidationError, QuarantineError
import requests
import hashlib
import redis
from dateutil import parser
from bs4 import BeautifulSoup
from pydantic import BaseModel, ValidationError, UUID5, StrictBool, PastDatetime, PositiveInt, PositiveFloat, Field

load_dotenv()
# Load System ENV VARS
READABILITY_FIELD = os.getenv('FIELD_FOR_READABILITY')
GET_TOKENS = os.getenv('GET_TOKENS').split(",")
FIELD_TO_VECTOR = os.getenv('PODCAST_VECTOR')
MIN_DESCRIPTION_LENGTH = int(os.getenv('MIN_PODCAST_DESC_LENGTH'))
MIN_TITLE_LENGTH = int(os.getenv('MIN_PODCAST_TITLE_LENGTH'))
REDIS_HOST = os.getenv('REDIS_HOST')
UUID_NAMESPACE = os.getenv('UUID_NAMESPACE')
TRUE_BOOL = os.getenv('TRUE_BOOL').split(",")
FALSE_BOOL = os.getenv('FALSE_BOOL').split(",")


class Episode(BaseModel):
    episode_uuid: UUID5
    episode_url: str
    podcast_id: PositiveInt
    duration: PositiveInt
    file_type: str
    language: str
    is_explicit: StrictBool
    publisher: str
    # "image_url": record['artwork_thumbnail'],
    description_cleaned: str
    title_cleaned: str
    readability: int
    description_selected: PositiveInt
    advanced_popularity: PositiveFloat
    index_status: PositiveInt
    record_hash: str
    publish_date: Optional[PastDatetime]


class EpisodeConsumer(threading.Thread):
    def __init__(self, jobs_q, quality_q, errors_q, purgatory_q, quarantine_q, thread_lock, nlp, model, *args,
                 **kwargs):
        self.nlp = nlp
        self.thread_lock = thread_lock
        self.logger = ErrorLogger(thread_lock, errors_q, purgatory_q, quarantine_q)
        self.model = model
        self.job_queue = jobs_q
        self.quality_q = quality_q
        self.thread_lock = thread_lock
        self.redis_cli = redis.Redis(host=REDIS_HOST, port=6379, charset="utf-8", decode_responses=True)
        self.namespace = uuid.uuid5(uuid.NAMESPACE_DNS, UUID_NAMESPACE)
        super().__init__(*args, **kwargs)

    def run(self):
        while True:
            try:
                task = self.job_queue.get(timeout=30)
                self.process(task)
            except queue.Empty:
                return

    @staticmethod
    def get_from_web(file_path):
        try:
            rss = requests.get(file_path, timeout=2.0)
            # return rss.text.encode()
            return rss.text
        except Exception:
            raise

    def validate_minimums(self, msg):
        try:
            description_len = len(msg['description_cleaned'].split(' '))
            title_len = len(msg['title_cleaned'].split(' '))
            if description_len < MIN_DESCRIPTION_LENGTH or title_len < MIN_TITLE_LENGTH:
                raise TypeError(
                    f"Minimum length(s) not met: title {title_len}:{MIN_TITLE_LENGTH}, description {description_len}:{MIN_DESCRIPTION_LENGTH}.",
                    msg)
        except Exception:
            raise

    def get_search_fields(self, kafka_message):
        for key in GET_TOKENS:
            lemma_key = f"{key.split('_')[0]}_lemma"
            kafka_message[lemma_key] = ProcessText.return_lemma(kafka_message[key], kafka_message['language'])
        kafka_message['vector'] = pickle.dumps(ProcessText.get_vector(kafka_message[FIELD_TO_VECTOR], self.model))

    def to_bool(self, string_bool, default):
        if string_bool is True or string_bool.lower() in TRUE_BOOL:
            return True
        elif string_bool is False or string_bool.lower() in FALSE_BOOL:
            return False
        else:
            return string_bool
            # return default

    def get_message(self, item, kafka_message):
        try:
            return {
                "episode_uuid": str(uuid.uuid5(self.namespace, item.enclosure.get('url'))),
                "episode_url": item.enclosure.get('url'),
                "podcast_id": kafka_message['podcast_id'],
                "duration": int(item.enclosure.get('length')),
                "file_type": item.enclosure.get('type'),
                "language": kafka_message['language'],
                "is_explicit": self.to_bool(item.explicit.get_text(),
                                            kafka_message['is_explicit']) if item.explicit else kafka_message[
                    'is_explicit'],
                "publisher": item.author.get_text() if item.author else kafka_message['publisher'],
                # episode_message["image_url": record['artwork_thumbnail'],
                "description_cleaned": ProcessText.return_clean_text(
                    item.description.get_text()) if item.description else '',
                "title_cleaned": ProcessText.return_clean_text(
                    item.title.get_text()) if item.title else '',
                "readability": 0,
                "description_selected": 410,
                "advanced_popularity": 1,
                "index_status": 310,
                'record_hash': hashlib.md5(str(item).encode()).hexdigest(),
                "publish_date": int(
                    parser.parse(item.pubDate.get_text()).timestamp()) if item.pubDate else None
            }
        except Exception:
            raise

    def process(self, kafka_message):
        try:
            rss = self.get_from_web(kafka_message['rss_url'])
            root = BeautifulSoup(rss, features="xml")  # More forgiving that lxml
            for item in root.findAll("item"):
                episode_message = self.get_message(item, kafka_message)
                Episode(**episode_message)
                self.validate_minimums(episode_message)

                # Check for Previous Instance in Redis
                previous_episode_uuid = self.redis_cli.get(episode_message['record_hash'])
                # Check for Exact Duplicates using hash of entire record string and podcast_uuid.
                if previous_episode_uuid == episode_message['episode_uuid']:
                    raise TypeError(
                        f"File {episode_message['episode_url']} is a duplicate to: {previous_episode_uuid}.",
                        episode_message)
                # Same Body Different podcast_uuid. title says "DELETED"??
                elif previous_episode_uuid:
                    raise QuarantineError({"episode_uuid": episode_message['episode_uuid'],
                                           "original_episode_uuid": previous_episode_uuid})
                # Set entry in Redis
                else:
                    self.redis_cli.set(episode_message['record_hash'], episode_message['episode_uuid'])
                    self.get_search_fields(episode_message)
                    if episode_message['language'] == 'en':
                        episode_message['readability'] = ProcessText.get_readability(episode_message[READABILITY_FIELD],
                                                                                     self.nlp)
                    with self.thread_lock:
                        self.quality_q.put(episode_message)

        except TypeError as err:
            # print(traceback.format_exc())
            error, message = err.args
            self.logger.log_to_purgatory(message, str(error))
        except QuarantineError as quarantine_obj:
            self.logger.log_to_quarantine(quarantine_obj)
        except ValidationError as err:
            self.logger.log_to_purgatory(episode_message, str(err.errors()))
        except Exception as err:
            self.logger.log_to_errors(kafka_message['rss_url'], str(err), traceback.format_exc(), 1)
