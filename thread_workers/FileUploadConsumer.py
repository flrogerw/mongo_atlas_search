import os
import threading
import queue
import traceback
import requests
import boto3
from requests.adapters import HTTPAdapter
from logger.Logger import ErrorLogger
from dotenv import load_dotenv
from urllib3.exceptions import InsecureRequestWarning, ConnectionError

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
adapter = HTTPAdapter(max_retries=2)
load_dotenv()
DEFAULT_IMAGE_PATH = os.getenv('DEFAULT_IMAGE_PATH')


class FileUploadConsumer(threading.Thread):
    def __init__(self,
                 jobs_q,
                 update_q,
                 errors_q,
                 thread_lock,
                 *args,
                 **kwargs):

        self.req_session = requests.Session()
        self.req_session.mount('http://', adapter)
        self.req_session.mount('https://', adapter)
        self.logger = ErrorLogger(thread_lock, errors_q, None, None)
        self.job_queue = jobs_q
        self.errors_q = errors_q
        self.update_q = update_q
        self.thread_lock = thread_lock
        self.s3 = boto3.client('s3')
        super().__init__(*args, **kwargs)

    def run(self):
        while True:
            try:
                task = self.job_queue.get(timeout=30)
                self.process(task)
            except queue.Empty:
                return

    def transfer_file(self, kafka_message):
        try:
            # If we can store xml as bytes I can remove this part
            if kafka_message['mime_type'] == 'application/rss+xml':
                res = self.req_session.get(kafka_message['url'], timeout=2.0, verify=False)
                if res.status_code == 200:
                    self.s3.put_object(
                        Body=res.text,
                        Bucket=kafka_message['upload_bucket'],
                        Key=f"{kafka_message['file_path']}/{kafka_message['file_name']}",
                        ContentType='application/rss+xml')
                else:
                    self.update_q.put(dict({
                        'podcast_uuid': kafka_message['podcast_uuid'],
                        'rss_url': f"URL returned a {res.status_code} error",

                    }))

            else:
                res = self.req_session.get(kafka_message['url'], stream=True)
                if res.status_code == 200:
                    self.s3.upload_fileobj(
                        res.raw,
                        kafka_message['upload_bucket'],
                        f"{kafka_message['file_path']}/{kafka_message['file_name']}",
                        ExtraArgs={'ContentType': kafka_message['mime_type']})
                else:
                    self.update_q.put(dict({
                        'podcast_uuid': kafka_message['podcast_uuid'],
                        'image_url': DEFAULT_IMAGE_PATH
                    }))
        except ConnectionError:
            raise
        except Exception:
            raise

    def process(self, kafka_message):
        try:
            self.transfer_file(kafka_message)
        except Exception as err:
            # print(traceback.format_exc())
            self.logger.log_to_errors(kafka_message['url'], str(err), traceback.format_exc(), 1)
