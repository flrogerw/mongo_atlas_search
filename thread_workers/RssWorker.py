from Errors import QuarantineError, ValidationError
import boto3
from botocore.exceptions import ClientError
import os
import threading
import queue
import uuid
import redis
import pickle
import json
import hashlib
import requests
import io
import traceback
from dotenv import load_dotenv
from lxml import etree
from nlp.ProcessText import ProcessText

load_dotenv()

# Load System ENV VARS
FIELD_FOR_READABILITY = os.getenv('FIELD_FOR_READABILITY')
NICE_TO_HAVE = os.getenv('NICE_TO_HAVE').split(",")
GET_TOKENS = os.getenv('GET_TOKENS').split(",")
FIELD_TO_VECTOR = os.getenv('FIELD_TO_VECTOR')
FIELDS_TO_HASH = os.getenv('FIELDS_TO_HASH').split(",")
ELEMENTS_TO_PROCESS = os.getenv('ELEMENTS_TO_PROCESS').split(",")
UUID_NAMESPACE = os.getenv('UUID_NAMESPACE')
REQUIRED_ELEMENTS = os.getenv('REQUIRED_ELEMENTS').split(",")
MIN_DESCRIPTION_LENGTH = int(os.getenv('MIN_DESCRIPTION_LENGTH'))
MIN_TITLE_LENGTH = int(os.getenv('MIN_TITLE_LENGTH'))
LANGUAGES = os.getenv('LANGUAGES').split(",")
MIN_LANGUAGE_TOLERANCE = os.getenv('MIN_LANGUAGE_TOLERANCE')
BUCKET = os.getenv('BUCKET')
UPLOAD_BUCKET = os.getenv('UPLOAD_BUCKET')
PROFANITY_CHECK = os.getenv('PROFANITY_CHECK').split(",")
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_USER = os.getenv('REDIS_USER')
REDIS_PASS = os.getenv('REDIS_PASS')


class RssWorker(threading.Thread):
    def __init__(self, job_queue, good_queue, bad_queue, quarantine_queue, purgatory_queue, nlp, profanity, model,
                 fetcher_type, thread_lock,
                 *args, **kwargs):
        self.nlp = nlp
        self.fetcher = fetcher_type
        self.thread_lock = thread_lock
        self.model = model
        self.profanity = profanity
        self.complete_queue = good_queue
        self.purgatory_queue = purgatory_queue
        self.error_queue = bad_queue
        self.quarantine_queue = quarantine_queue
        self.job_queue = job_queue
        self.s3 = boto3.client("s3")
        self.redis = redis.Redis(host=REDIS_HOST, port=6379, charset="utf-8", decode_responses=True)
        self.namespace = uuid.uuid5(uuid.NAMESPACE_DNS, UUID_NAMESPACE)

        super().__init__(*args, **kwargs)

    def run(self):
        while True:
            try:
                task = self.job_queue.get()
                self.process(task)
            except queue.Empty:
                return
            self.job_queue.task_done()

    def get_from_s3(self, file_name):
        try:
            bytes_buffer = io.BytesIO()
            self.s3.download_fileobj(Bucket=BUCKET, Key=file_name, Fileobj=bytes_buffer)
            byte_value = bytes_buffer.getvalue()
            return byte_value
        except Exception:
            raise

    @staticmethod
    def get_from_web(file_path):
        try:
            rss = requests.get(file_path, timeout=2.0)
            return rss.text.encode()
        except Exception:
            raise

    def log_to_episodes(self, episode, xml, language):
        # print(episode_dict)
        # Check for required fields
        fields = ["title", "summary", "author", "enclosure", "pubDate", "enclosure", "explicit", "keywords"]
        """
        for field in fields:
            episode[field] = 'n/a'
            stub = xml.find(".//" + field)
        if hasattr(stub, 'text') and stub.text:
            episode[field] = ProcessText.return_clean_text(stub.text)

        log_entry = {
            "title_cleaned": episode['file_name'],
            "title_lemma": episode['file_name'],
            "description_cleaned": episode['file_name'],
            "description_lemma": episode['file_name'],
            "episode_hash": episode['file_hash'],
            "episode_uuid": episode['podcast_uuid'],
            "language": language,
            "episode_url": title,
            "publish_date": description,
            "author": author,
            "is_explicit": 320,
            "length": 320,
            "file_type": 320,
            "tags": 320,
            "vector": ''
        }
        with self.thread_lock:
            self.purgatory_queue.put(log_entry)
        """

    def log_to_quarantine(self, podcast_uuid, matching_uuid, file_name):
        # print(podcast_uuid, matching_uuid, file_name)
        with self.thread_lock:
            self.quarantine_queue.put({"podcast_uuid": podcast_uuid,
                                       "original_podcast_uuid": matching_uuid,
                                       "duplicate_file_name": file_name})

    def log_to_errors(self, file_name, error_str, stack_trace):
        # print(error_str, stack_trace)
        with self.thread_lock:
            self.error_queue.put({"file_name": file_name,
                                  "error": error_str,
                                  "stack_trace": stack_trace.replace("\x00", "\uFFFD")})

    def log_to_purgatory(self, response, xml, error):
        # print(error)
        try:
            title = 'n/a'
            description = 'n/a'
            author = 'n/a'

            xml_title = xml.find(".//channel/title")
            xml_description = xml.find(".//channel/description")
            xml_author = xml.find(".//channel/author")

            if hasattr(xml_title, 'text') and xml_title.text:
                title = ProcessText.return_clean_text(xml_title.text)
            if hasattr(xml_description, 'text') and xml_description.text:
                description = ProcessText.return_clean_text(xml_description.text)
            if hasattr(xml_author, 'text') and xml_author.text:
                author = ProcessText.return_clean_text(xml_author.text)

            log_entry = {
                "file_name": response['file_name'],
                "file_hash": response['file_hash'],
                "podcast_uuid": response['podcast_uuid'],
                "language": response['language'],
                "rss_url": response['rss_url'],
                "reason_for_failure": error,
                "title_cleaned": title,
                "description_cleaned": description,
                "author": author,
                "index_status": 320
            }
            with self.thread_lock:
                self.purgatory_queue.put(log_entry)
        except Exception:
            raise

    @staticmethod
    def get_extra_fields(xml, response):
        for field in NICE_TO_HAVE:
            nth_field = xml.find(".//channel/" + field)
            if hasattr(nth_field, 'text'):
                field = field.replace('/', '_')
                clean_text = ProcessText.return_clean_text(nth_field.text)
                response[field] = clean_text if not None else 'n/a'

    @staticmethod
    def validate_xml(xml):
        try:
            # Make sure all required elements are present
            for element in REQUIRED_ELEMENTS:
                if not hasattr(xml.find(".//channel/" + element), 'text'):
                    raise ValidationError("RSS is missing required element: {}.".format(element))
        except Exception:
            raise

    @staticmethod
    def validate_text_length(response):
        try:
            description_len = len(response['description_cleaned'].split(' '))
            title_len = len(response['title_cleaned'].split(' '))

            if description_len < MIN_DESCRIPTION_LENGTH or title_len < MIN_TITLE_LENGTH:
                raise ValidationError(
                    "Minimum length(s) not met: title {}:{}, description {}:{}."
                    .format(title_len, MIN_TITLE_LENGTH, description_len,
                            MIN_DESCRIPTION_LENGTH))
        except Exception:
            raise

    @staticmethod
    def process_search_fields(xml, response):
        for e in ELEMENTS_TO_PROCESS:
            clean_text = ProcessText(xml.find(".//channel/" + e).text, response['language'])
            response[e + "_cleaned"] = clean_text.get_clean()
            if e in GET_TOKENS:
                response[e + '_lemma'] = clean_text.get_tokens()

    def process_episodes(self, root, podcast_uuid):
        try:
            for item in root.iter("item"):
                episode = {'episode_hash': str(uuid.uuid5(self.namespace, str(item))), "podcast_uuid": podcast_uuid}
                if hasattr(item, 'text'):
                    print(item.text)
                self.log_to_episodes(episode)
        except Exception:
            raise

    def process(self, task):
        root = '<?xml version="1.0" encoding="UTF-8"?>'
        # Populate fields that may or may not get populated below.  We need to keep the structure
        # consistent, so we can use the bulk insert function of psycopg2.
        response = {"readability": 0, "index_status": 310, "is_deleted": False, "advanced_popularity": 1,
                    "description_selected": 0, "author": 'n/a', "description_chatgpt": 'n/a', "image_url": 'n/a',
                    "language": 'n/a'}
        try:
            if self.fetcher == 'pubsub':
                xml = self.get_from_s3(task['feedFilePath'])
                root = etree.XML(xml)
                response['file_hash'] = hashlib.md5(str(xml).encode()).hexdigest()
                response['file_name'] = task['feedFilePath']

            elif self.fetcher == 'listen_notes':
                xml = self.get_from_web(task['feedFilePath'])
                response['podcast_uuid'] = str(uuid.uuid5(self.namespace, task['feedFilePath']))
                response['file_hash'] = hashlib.md5(str(xml).encode()).hexdigest()
                response['file_name'] = response['file_hash'] + '.rss.xml'
                response['rss_url'] = task['feedFilePath']
                root = etree.XML(xml)

            previous_podcast_uuid = self.redis.get(response['file_hash'])
            # Check for Exact Duplicates using hash of entire file string and hash of URL.
            if previous_podcast_uuid is not None and previous_podcast_uuid == response['podcast_uuid']:
                raise ValidationError(
                    "File {} is a duplicate to: {}.".format(task['feedFilePath'], previous_podcast_uuid))
            # Same Body Different URL. title says "DELETED"??
            if previous_podcast_uuid == response['podcast_uuid']:
                raise QuarantineError(previous_podcast_uuid)
            # Set entry in Redis
            else:
                self.redis.set(response['file_hash'], response['podcast_uuid'])

            # Make sure the doc meets basic acceptance criteria.
            self.validate_xml(root)

            # Set some basic values
            response['language'] = ProcessText.get_language(root, self.nlp, MIN_LANGUAGE_TOLERANCE, LANGUAGES)
            response['episode_count'] = len(list(root.iter("item")))
            # Do the processing
            self.process_search_fields(root, response)
            self.validate_text_length(response)
            self.get_extra_fields(root, response)
            # self.process_episodes(root, response['podcast_uuid']):
            # Get Explicit rating
            response['is_explicit'] = ProcessText.profanity_check(response, PROFANITY_CHECK, self.profanity)
            # Save the heavy lifting for last when we are sure everything is valid.
            response['vector'] = pickle.dumps(ProcessText.get_vector(response[FIELD_TO_VECTOR], self.model))

            # Limited by Language Functions
            if response['language'] == 'en':
                response['readability'] = ProcessText.get_readability(response[FIELD_FOR_READABILITY], self.nlp)

            # Post response in the completed queue and S3.
            # if self.fetcher == 'listen_notes':
            # self.s3.put_object(Body=str(xml), Bucket=UPLOAD_BUCKET, Key=response['file_name'])

            with self.thread_lock:
                self.complete_queue.put(response)

        except ClientError as err:
            self.log_to_errors(task['feedFilePath'], str(err), traceback.format_exc())

        except ValidationError as err:
            self.log_to_purgatory(response, root, str(err))

        except QuarantineError as previous_podcast_uuid:
            self.log_to_quarantine(response['podcast_uuid'], str(previous_podcast_uuid), task['feedFilePath'])

        except Exception as err:
            self.log_to_errors(task['feedFilePath'], str(err), traceback.format_exc())
