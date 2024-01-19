from urllib3 import Retry, HTTPConnectionPool, Timeout
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
from nlp.Grader import Grader

load_dotenv()
xmlschema = etree.XMLSchema(etree.parse('xsd/rss.xsd'))
timeout = Timeout(connect=2.0, read=2.0)
retries = Retry(connect=0, read=2, redirect=5)
http = HTTPConnectionPool('localhost', retries=retries, timeout=timeout)

# Load System ENV VARS
FIELD_FOR_READABILITY = os.getenv('FIELD_FOR_READABILITY')
NICE_TO_HAVE = os.getenv('NICE_TO_HAVE').split(",")
GET_TOKENS = os.getenv('GET_TOKENS').split(",")
FIELD_TO_VECTOR = os.getenv('FIELD_TO_VECTOR')
FIELDS_TO_HASH = os.getenv('FIELDS_TO_HASH').split(",")
ELEMENTS_TO_PROCESS = os.getenv('ELEMENTS_TO_PROCESS').split(",")
UUID_NAMESPACE = os.getenv('UUID_NAMESPACE')
REQUIRED_ELEMENTS = os.getenv('REQUIRED_ELEMENTS').split(",")
MIN_DESCRIPTION_LENGTH = os.getenv('MIN_DESCRIPTION_LENGTH')
MIN_TITLE_LENGTH = os.getenv('MIN_TITLE_LENGTH')
LANGUAGES = os.getenv('LANGUAGES').split(",")
MIN_LANGUAGE_TOLERANCE = os.getenv('MIN_LANGUAGE_TOLERANCE')
RESPONSE_OBJECT = os.getenv('RESPONSE_OBJECT').split(",")
BUCKET = os.getenv('BUCKET')
UPLOAD_BUCKET = os.getenv('UPLOAD_BUCKET')
PROFANITY_CHECK = os.getenv('PROFANITY_CHECK').split(",")


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
        self.redis = redis.Redis(host='localhost', port=6379, charset="utf-8", decode_responses=True)
        self.namespace = uuid.uuid5(uuid.NAMESPACE_DNS, UUID_NAMESPACE)
        words = open('nlp/bad_word_list.json')
        self.bad_words = json.load(words)

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

    def log_to_quarantine(self, podcast_uuid, matching_uuid, file_name):
        with self.thread_lock:
            self.quarantine_queue.put({"podcast_uuid": podcast_uuid,
                                       "original_podcast_uuid": matching_uuid,
                                       "duplicate_file_name": file_name})

    def log_to_purgatory(self, response, xml, error):
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

    def get_language_from_model(self, text):
        clean_text = ProcessText.return_clean_text(text)
        doc = self.nlp(clean_text)
        dl = doc._.language
        return dl["language"], dl["score"]

    def get_readability(self, text, grader_type='dale_chall'):
        grader = Grader(text, self.nlp)
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

    def is_duplicate(self, response):
        try:
            duplicate_check = self.redis.get(response['podcast_uuid'])
            if duplicate_check is not None:
                raise ValidationError("Has the same URL as this current podcast: {}.".format(duplicate_check))
            else:
                self.redis.set(response['podcast_uuid'], response['rss_url'])
        except Exception:
            raise

    @staticmethod
    def validate_xml(xml):
        try:
            if xmlschema.validate(xml) is False:
                raise ValidationError("RSS is missing rss or channel element(s.)")

            # Make sure all required elements are present
            for element in REQUIRED_ELEMENTS:
                if not hasattr(xml.find(".//" + element), 'text'):
                    raise ValidationError("RSS is missing required element: {}.".format(element))
        except Exception:
            raise

    @staticmethod
    def validate_response(response):
        try:
            description_len = len(response['description_cleaned'].split(' '))
            title_len = len(response['title_cleaned'].split(' '))

            if description_len < int(MIN_DESCRIPTION_LENGTH) or title_len < int(MIN_TITLE_LENGTH):
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

    def process_episodes(self, root):
        try:
            for item in root.iter("item"):
                epidose_id = str(uuid.uuid5(self.namespace, str(item)))
                if item is not None:
                    print(item.text)
        except Exception:
            raise

    def get_language(self, root):
        # Filter out unsupported languages
        language = root.find(".//language")
        if hasattr(language, 'text'):
            language = language.text.lower().split('-')[0]
        else:
            language_text = root.find(".//description")
            if hasattr(language_text, 'text'):
                get_lang = self.get_language_from_model(language_text.text)
                if get_lang is None:
                    raise ValidationError("Language not supported: {}.".format(language))
                language, tolerance = get_lang
                if tolerance < float(MIN_LANGUAGE_TOLERANCE):
                    raise ValidationError("Minimum Language Tolerance not met {}:{}.".format(tolerance,
                                                                                        MIN_LANGUAGE_TOLERANCE))
            else:
                raise ValidationError("Can Not Determine Language.")
        if language not in LANGUAGES:
            raise ValidationError("Language not supported: {}.".format(language))
        else:
            return language

    def process(self, task):
        root = '<?xml version="1.0" encoding="UTF-8"?>'
        response = {"readability": 0, "index_status": 310, "is_deleted": False, "advanced_popularity": 1,
                    "author": 'n/a', "description_chatgpt": 'n/a', "image_url": 'n/a'}

        try:
            if self.fetcher == 'kafka':
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

            file_hash = self.redis.get(response['file_hash'])
            # Check for Exact Duplicates using hash of entire file string and hash of URL.
            if file_hash is not None and file_hash == response['podcast_uuid']:
                raise ValidationError("File {} is a duplicate to: {}.".format(task['feedFilePath'], file_hash))
            # SAME BODY DIFFERENT URL title says "DELETED"??
            if file_hash:
                raise QuarantineError(file_hash)
            # Set entry in Redis
            else:
                self.redis.set(response['file_hash'], response['podcast_uuid'])

            # Make sure the doc meets basic acceptance criteria.
            self.validate_xml(root)

            # Set some basic values
            response['language'] = self.get_language(root)
            response['episode_count'] = len(list(root.iter("item")))
            response['description_selected'] = 0  # ADD coherence test and use chatagpt if fails

            self.process_search_fields(root, response)
            self.validate_response(response)
            self.is_duplicate(response)
            self.get_extra_fields(root, response)
            response['is_explicit'] = ProcessText.profanity_check(response, PROFANITY_CHECK, self.profanity)
            # Save the heavy lifting for last when we are sure everything is valid.
            response['vector'] = pickle.dumps(ProcessText.get_vector(response[FIELD_TO_VECTOR], self.model))

            # English Only Functions
            if response['language'] == 'en':
                response['readability'] = self.get_readability(response[FIELD_FOR_READABILITY])

            # Post response in the completed queue and S3.
            # if self.fetcher == 'listen_notes':
            # self.s3.put_object(Body=str(xml), Bucket=UPLOAD_BUCKET, Key=response['file_name'])

            with self.thread_lock:
                self.complete_queue.put(response)

        except ClientError as err:
            print(err)
            with self.thread_lock:
                self.error_queue.put(
                    {"file_name": response['file_name'], "error": traceback.format_exc().replace("\x00", "\uFFFD")})

        except ValidationError as err:
            # print(err)
            self.log_to_purgatory(response, root, str(err))

        except QuarantineError as err:
            self.log_to_quarantine(response['podcast_uuid'], str(err), task['feedFilePath'])

        except Exception as err:
            # print(err)
            with self.thread_lock:
                self.error_queue.put(
                    {"file_name": task['feedFilePath'], "error": str(err), "stack_trace": traceback.format_exc().replace("\x00", "\uFFFD")})
