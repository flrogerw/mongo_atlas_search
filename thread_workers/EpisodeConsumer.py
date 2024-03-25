import os
import pickle
import uuid
import threading
import queue
import traceback
import requests
import hashlib
import redis
from nlp.Grader import Grader
from dotenv import load_dotenv
from logger.Logger import ErrorLogger
from schemas.validation import Episode
from Errors import QuarantineError
from dateutil import parser
from bs4 import BeautifulSoup

# Load System ENV VARS
load_dotenv()
READABILITY_FIELD = os.getenv('FIELD_FOR_READABILITY')
GET_TOKENS = os.getenv('GET_TOKENS').split(",")
FIELDS_TO_VECTOR = os.getenv('EPISODE_FIELDS_TO_VECTOR').split(",")
MIN_DESCRIPTION_LENGTH = int(os.getenv('MIN_PODCAST_DESC_LENGTH'))
MIN_TITLE_LENGTH = int(os.getenv('MIN_PODCAST_TITLE_LENGTH'))
REDIS_HOST = os.getenv('REDIS_HOST')
UUID_NAMESPACE = os.getenv('UUID_NAMESPACE')
TRUE_BOOL = os.getenv('TRUE_BOOL').split(",")
FALSE_BOOL = os.getenv('FALSE_BOOL').split(",")


class EpisodeConsumer(threading.Thread):
    def __init__(self,
                 jobs_q,
                 quality_q,
                 errors_q,
                 purgatory_q,
                 quarantine_q,
                 thread_lock,
                 nlp,
                 model,
                 *args,
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
        self.entity_type = 'episode'
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
                    f"Minimum length(s) not met: title {title_len}:{MIN_TITLE_LENGTH}, description {description_len}:{MIN_DESCRIPTION_LENGTH}.")
        except Exception:
            raise

    def get_field_lemmas(self, message):
        try:
            for key in GET_TOKENS:
                lemma_key = f"{key.split('_')[0]}_lemma"
                message[lemma_key] = self.nlp.get_lemma(message[key], message['language'])
        except Exception:
            raise

    def get_field_vectors(self, kafka_message):
        try:
            for key in FIELDS_TO_VECTOR:
                vector_key = f"{key.split('_')[0]}_vector"
                kafka_message[vector_key] = pickle.dumps(self.nlp.get_vector(kafka_message[key], self.model))
        except Exception:
            raise

    def to_bool(self, string_bool, default):
        if string_bool is True or string_bool.lower() in TRUE_BOOL:
            return True
        elif string_bool is False or string_bool.lower() in FALSE_BOOL:
            return False
        else:
            return string_bool

    def get_guid(self, guid_str):
        try:
            uuid.UUID(str(guid_str))
            return guid_str
        except ValueError:
            return str(uuid.uuid5(self.namespace, guid_str))

    def get_message(self, item, kafka_message):
        try:
            if hasattr(item.guid, 'get_text'):
                episode_uuid = self.get_guid(str(item.guid.get_text()))
            else:
                episode_uuid = str(uuid.uuid5(self.namespace, item.enclosure.get('url')))

            message = {
                "episode_uuid": episode_uuid,
                "episode_url": item.enclosure.get('url'),
                "podcast_id": kafka_message['podcast_id'],
                "duration": int(item.enclosure.get('length')) if item.enclosure.get('length') else 0,
                "file_type": item.enclosure.get('type'),
                "language": kafka_message['language'],
                "is_explicit": self.to_bool(item.explicit.get_text(),
                                            kafka_message['is_explicit']) if item.explicit else kafka_message[
                    'is_explicit'],
                "publisher": item.author.get_text() if item.author else kafka_message['publisher'],
                "image_url": kafka_message['image_url'],
                "description_cleaned": self.nlp.clean_text(
                    item.description.get_text()) if item.description else '',
                "title_cleaned": self.nlp.clean_text(
                    item.title.get_text()) if item.title else '',
                "readability": 0,
                "description_selected": 410,
                "advanced_popularity": 0,
                "index_status": 310,
                "publish_date": int(
                    parser.parse(item.pubDate.get_text()).timestamp()) if item.pubDate else None
            }
            message['record_hash'] = hashlib.md5(str(message).encode()).hexdigest()
            return message
        except Exception:
            raise

    def get_readability(self, text_str, language='en', grader_type='dale_chall'):
        try:
            grader = Grader(self.nlp.text_processors[language](text_str))
            if grader_type == 'dale_chall':
                return grader.dale_chall_readability_score()
            elif grader_type == 'flesch_kincaid':
                return grader.flesch_kincaid_readability_test()
            elif grader_type == 'gunning_fog':
                return grader.gunning_fog()
            elif grader_type == 'smog_index':
                return grader.smog_index()
            else:
                return 0
        except Exception:
            raise

    def process(self, kafka_message):
        try:
            rss = self.get_from_web(kafka_message['rss_url'])
            root = BeautifulSoup(rss, features="xml")  # More forgiving that lxml
            for item in root.findAll("item"):
                episode_message = self.get_message(item, kafka_message)

                # self.validate_minimums(episode_message)

                # Check for Previous Instance in Redis
                previous_episode_hash = self.redis_cli.get(f"{self.entity_type}_{episode_message['episode_uuid']}")
                # Check for Exact Duplicates using hash of entire record string and podcast_uuid.
                if previous_episode_hash == episode_message['episode_uuid']:
                    raise TypeError(
                        f"File {episode_message['episode_url']} is a duplicate to: {previous_episode_hash}.")
                # Same Body Different podcast_uuid. title says "DELETED"??
                elif previous_episode_hash:
                    raise QuarantineError({"episode_uuid": episode_message['episode_uuid'],
                                           "duplicate_file_name": episode_message['episode_url'],
                                           "original_episode_hash": previous_episode_hash})
                # Set entry in Redis
                else:
                    self.redis_cli.set(f"{self.entity_type}_{episode_message['episode_uuid']}",
                                       episode_message['record_hash'])
                    self.get_field_vectors(episode_message)
                    self.get_field_lemmas(episode_message)
                    if episode_message['language'] == 'en' and len(episode_message[READABILITY_FIELD]) > 5:
                        episode_message['readability'] = self.get_readability(episode_message[READABILITY_FIELD])
                    with self.thread_lock:
                        self.quality_q.put(episode_message)

        except TypeError as err:
            # print(traceback.format_exc())
            self.logger.log_to_purgatory(episode_message, str(err))
        except QuarantineError as quarantine_obj:
            self.logger.log_to_quarantine(quarantine_obj.args[0])
        except ValueError as err:
            self.logger.log_to_purgatory(episode_message, str(err))
        except Exception as err:
            self.logger.log_to_errors(kafka_message['rss_url'], str(err), traceback.format_exc(), 520)
