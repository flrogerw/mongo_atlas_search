import os
import pickle
import threading
import queue
import traceback
from nlp.ProcessText import ProcessText
from dotenv import load_dotenv
from logger.Logger import ErrorLogger
from Errors import ValidationError, QuarantineError
import requests
from lxml import etree
import hashlib
import redis
from dateutil import parser

load_dotenv()
# Load System ENV VARS
READABILITY_FIELD = os.getenv('FIELD_FOR_READABILITY')
GET_TOKENS = os.getenv('GET_TOKENS').split(",")
FIELD_TO_VECTOR = os.getenv('PODCAST_VECTOR')
MIN_DESCRIPTION_LENGTH = int(os.getenv('MIN_PODCAST_DESC_LENGTH'))
MIN_TITLE_LENGTH = int(os.getenv('MIN_PODCAST_TITLE_LENGTH'))
REDIS_HOST = os.getenv('REDIS_HOST')


class EpisodeConsumer(threading.Thread):
    def __init__(self, jobs_q, quality_q, errors_q, thread_lock, nlp, model, *args, **kwargs):
        self.nlp = nlp
        self.thread_lock = thread_lock
        self.logger = ErrorLogger(thread_lock, errors_q)
        self.model = model
        self.job_queue = jobs_q
        self.quality_q = quality_q
        self.thread_lock = thread_lock
        self.redis_cli = redis.Redis(host=REDIS_HOST, port=6379, charset="utf-8", decode_responses=True)
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
            return rss.text.encode()
        except Exception:
            raise

    def validate_minimums(self, msg):
        try:
            description_len = len(msg['description_cleaned'].split(' '))
            title_len = len(msg['title_cleaned'].split(' '))
            if description_len < MIN_DESCRIPTION_LENGTH or title_len < MIN_TITLE_LENGTH:
                raise ValidationError(
                    f"Minimum length(s) not met: title {title_len}:{MIN_TITLE_LENGTH}, description {description_len}:{MIN_DESCRIPTION_LENGTH}.")
        except Exception:
            raise

    def get_search_fields(self, kafka_message):
        for key in GET_TOKENS:
            lemma_key = f"{key.split('_')[0]}_lemma"
            kafka_message[lemma_key] = ProcessText.return_lemma(kafka_message[key], kafka_message['language'])
        kafka_message['vector'] = pickle.dumps(ProcessText.get_vector(kafka_message[FIELD_TO_VECTOR], self.model))

    def process(self, kafka_message):
        try:
            rss = self.get_from_web(kafka_message['rss_url'])
            root = etree.fromstring(rss)
            for item in root.iter("item"):
                episode_message = {
                    "episode-uuid": "XXXXXX",
                    "episode_url": item.find("enclosure").attrib['url'],
                    "duration": item.find("enclosure").attrib['length'],
                    "file_type": item.find("enclosure").attrib['type'],
                    "language": kafka_message['language'],
                    "is_explicit": item.find("explicit").text
                    if hasattr(item.find("explicit"), 'text') else kafka_message['is_explicit'],
                    "podcast_uuid": kafka_message['podcast_uuid'],
                    "publisher": item.find("author").text
                    if hasattr(item.find("author"), 'text') else kafka_message['publisher'],
                    # "image_url": record['artwork_thumbnail'],
                    "description_cleaned": ProcessText.return_clean_text(item.find("description").text),
                    "title_cleaned": ProcessText.return_clean_text(item.find("title").text),
                    "readability": 0,
                    "description_selected": 110,
                    "advanced_popularity": 1,
                    "publish_date": int(parser.parse(item.find("pubDate").text).timestamp())
                    if hasattr(item.find("pubDate"), 'text') else None,
                    "record_hash": hashlib.md5(str(rss).encode()).hexdigest()}

                # Check for Previous Instance in Redis
                previous_podcast_uuid = self.redis_cli.get(episode_message['record_hash'])
                # Check for Exact Duplicates using hash of entire record string and podcast_uuid.
                if previous_podcast_uuid == episode_message['podcast_uuid']:
                    continue
                # Same Body Different podcast_uuid. title says "DELETED"??
                elif previous_podcast_uuid:
                    raise QuarantineError(previous_podcast_uuid)
                # Set entry in Redis
                else:
                    self.redis_cli.set(episode_message['record_hash'], episode_message['podcast_uuid'])

                self.get_search_fields(episode_message)

                with self.thread_lock:
                    self.quality_q.put(episode_message)

        except ValidationError as err:
            # print(traceback.format_exc())
            self.logger.log_to_purgatory(episode_message, str(err))
        except QuarantineError as previous_podcast_uuid:
            self.logger.log_to_quarantine(episode_message, str(previous_podcast_uuid))
        except Exception as err:
            self.logger.log_to_errors(kafka_message['rss_url'], err, traceback.format_exc(), 555)
