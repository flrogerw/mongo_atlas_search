from urllib3 import Retry, HTTPConnectionPool, Timeout
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
threadLock = threading.Lock()
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
                 fetcher_type, lock,
                 *args, **kwargs):
        self.nlp = nlp
        self.thread_lock = lock
        self.fetcher = fetcher_type
        self.model = model
        self.profanity = profanity
        self.complete_queue = good_queue
        self.purgatory_queue = purgatory_queue
        self.error_queue = bad_queue
        self.quarantine_queue = quarantine_queue
        self.job_queue = job_queue
        self.s3 = boto3.client("s3")
        self.redis = redis.Redis(host='localhost', port=6379, charset="utf-8", decode_responses=True)
        self.redis.select(2)  # internal hash storage
        self.namespace = uuid.uuid5(uuid.NAMESPACE_DNS, UUID_NAMESPACE)
        words = open('nlp/bad_word_list.json')
        self.bad_words = json.load(words)

        super().__init__(*args, **kwargs)

    def run(self):
        while True:
            try:
                with self.thread_lock:
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
            rss = requests.get(file_path, timeout=1.0)
            return rss.text.encode()
        except Exception:
            raise

    def log_quarantine(self, podcast_uuid, matching_uuid, file_name):
        self.quarantine_queue.put({"podcast_uuid": podcast_uuid,
                                   "original_podcast_uuid": matching_uuid,
                                   "duplicate_file_name": file_name})

    def log_failure_record(self, response, xml, error):
        try:
            title = xml.find(".//channel/title")
            description = xml.find(".//channel/description")
            author = xml.find(".//channel/author")
            with self.thread_lock:
                self.purgatory_queue.put(
                    {
                        "file_name": response['file_name'],
                        "reason_for_failure": error,
                        "title_cleaned": title.text if title is not None else 'N/A',
                        "description_cleaned": description.text if description is not None else 'N/A',
                        "author": author.text if author is not None else 'N/A',
                        "index_status": 320
                    })
        except Exception:
            raise

    def get_language(self, text):
        doc = self.nlp(text)
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
            #  If URL is the same, it is the same Podcast as per Ray
            if self.redis.get(response['md5_podcast_url']) is not None:
                raise ValueError("DUPLICATE_WARNING: Has the same URL as this current podcast: {}.".format(
                    self.redis.get(response['md5_podcast_url'])))

            # If title & description match but not the URL then...quarantine
            title = self.redis.get(response['md5_title'])
            description = self.redis.get(response['md5_description'])
            is_quarantined = self.redis.get(response['podcast_uuid'])

            if title is not None and description is not None and is_quarantined is None:
                self.redis.set(response['podcast_uuid'], title)
                self.log_quarantine(response['podcast_uuid'], title, response['file_name'])
                raise Exception(
                    "QUARANTINE_ERROR: Sent to Quarantine.  Matches Current Record: {}.".format(
                        self.redis.get(response['md5_description'])))
        except Exception:
            raise

    @staticmethod
    def validate(xml):
        try:
            # Make sure there is a channel element to parse
            if xmlschema.validate(xml) is False:
                raise ValueError("RSS is missing rss or channel element(s.)")

            # Make sure all required elements are present
            for e in REQUIRED_ELEMENTS:
                if xml.find(".//" + e) is None:
                    raise ValueError("VALIDATION_ERROR: RSS is missing required element: {}.".format(e))

            # Make sure the title and description are not too short.
            description_len = len(xml.find(".//description").text.split(' '))
            title_len = len(xml.find(".//title").text.split(' '))
            if description_len < int(MIN_DESCRIPTION_LENGTH) or title_len < int(MIN_TITLE_LENGTH):
                raise ValueError(
                    "VALIDATION_ERROR: Minimum length(s) not met: title {}:{}, description {}:{}."
                    .format(title_len, MIN_TITLE_LENGTH, description_len,
                            MIN_DESCRIPTION_LENGTH))
        except Exception:
            raise

    def process_episodes(self, root):
        try:
            for item in root.iter("item"):
                epidose_id = str(uuid.uuid5(self.namespace, str(item)))
                if item is not None:
                    print(item.text)
        except Exception:
            raise

    def process(self, task):
        response = dict.fromkeys(RESPONSE_OBJECT, False)
        root = '<?xml version="1.0" encoding="UTF-8"?>'
        try:
            if self.fetcher == 'kafka':
                xml = self.get_from_s3(task['feedFilePath'])
                response['file_name'] = task['feedFilePath']
                root = etree.XML(xml)
            elif self.fetcher == 'listen_notes':
                xml = self.get_from_web(task['feedFilePath'])
                response['file_name'] = str(uuid.uuid5(self.namespace, str(xml))) + '.rss.xml'
                root = etree.XML(xml)

            # Check for Duplicates
            if self.redis.get(str(uuid.uuid5(self.namespace, str(xml)))) is None:
                self.redis.set(str(uuid.uuid5(self.namespace, str(xml))), task['feedFilePath'])
            else:
                raise ValueError("DUPLICATE_ERROR: File {} is a duplicate to: {}."
                                 .format(task['feedFilePath'],
                                         self.redis.get(str(uuid.uuid5(self.namespace, str(xml))))))

            #if self.fetcher == 'listen_notes':
                #self.s3.put_object(Body=str(xml), Bucket=UPLOAD_BUCKET, Key=response['file_name'])


            # Filter out unsupported languages
            language = root.find(".//language").text.lower().split('-')[0]
            if language is None:
                language_tuple = self.get_language
                if language_tuple[0] in LANGUAGES and language_tuple[1] > float(MIN_LANGUAGE_TOLERANCE):
                    language = language_tuple[0].lower().split('-')[0]
            if language not in LANGUAGES:
                # LOG AS 'excluded' and insert into DB
                raise ValueError("VALIDATION_ERROR: Language not supported: {}.".format(language))
            else:
                response['language'] = language

            # Make sure the doc meets acceptance criteria.
            self.validate(root)
            channel = root.find(".//channel")

            # Set some basic values
            response['podcast_url'] = root.find(".//link").text
            response['index_status'] = 310
            response['episode_count'] = len(list(root.iter("item")))
            # ADD coherence test and use chatagpt if fails
            response['description_selected'] = 0

            # Populate nice to have variables
            for field in NICE_TO_HAVE:
                if channel.find(".//" + field) is not None:
                    text = ProcessText(channel.find(".//" + field).text, self.model, response['language'])
                    field = field.replace('/', '_')
                    response[field] = text.get_clean()

            # PreProcess text
            for e in ELEMENTS_TO_PROCESS:
                clean_text = ProcessText(channel.find(".//" + e).text, self.model, response['language'])
                response[e + "_cleaned"] = clean_text.get_clean()
                if e in GET_TOKENS:
                    response[e + '_lemma'] = clean_text.get_tokens()
                if e == FIELD_TO_VECTOR:
                    response[FIELD_TO_VECTOR + '_vector'] = pickle.dumps(clean_text.get_vector())

            # Hash identifier fields
            fields_to_hash = list(map(lambda a, r=response: r[a], FIELDS_TO_HASH))
            concat_str = "|".join(fields_to_hash)
            response['podcast_uuid'] = str(uuid.uuid5(self.namespace, concat_str))

            for field in FIELDS_TO_HASH:
                short_field = field.replace('_cleaned', '')
                response['md5_' + short_field] = hashlib.md5(response[field].encode()).hexdigest()

            self.is_duplicate(response)

            self.redis.set(response['md5_podcast_url'], response['podcast_uuid'])
            self.redis.set('md5_description', response['podcast_uuid'])
            self.redis.set('md5_title', response['podcast_uuid'])

            # Check for Profanity
            bad_words = self.bad_words[response['language']]
            profanity_check_str = ' '.join(
                list(map(lambda a, r=response: r[a], PROFANITY_CHECK)))
            self.profanity.load_censor_words(bad_words)
            response['is_explicit'] = int(self.profanity.contains_profanity(profanity_check_str))

            # English Only Functions
            if response['language'] == 'en':
                response['readability'] = self.get_readability(response[FIELD_FOR_READABILITY])

            # Post response in the completed queue.
            with self.thread_lock:
                self.complete_queue.put(response)

        except ClientError as err:
            print(err)
            with self.thread_lock:
                self.error_queue.put(
                    {"file_name": response['file_name'], "error": traceback.format_exc()})

        except ValueError as err:
            # print(err)
            self.log_failure_record(response, root, str(err))

        except Exception as err:
            # print(err)
            with self.thread_lock:
                self.error_queue.put(
                    {"file_name": task['feedFilePath'], "error": str(err), "stack_trace": traceback.format_exc()})
