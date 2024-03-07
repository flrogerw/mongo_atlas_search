import threading
import queue
import traceback
import requests
import boto3
from botocore.exceptions import NoCredentialsError
from logger.Logger import ErrorLogger


class ImageConsumer(threading.Thread):
    def __init__(self,
                 jobs_q,
                 update_q,
                 errors_q,
                 thread_lock,
                 *args,
                 **kwargs):

        self.logger = ErrorLogger(thread_lock, errors_q, None, None)
        self.job_queue = jobs_q
        self.errors_q = errors_q
        self.update_q = update_q
        self.thread_lock = thread_lock
        super().__init__(*args, **kwargs)

    def run(self):
        while True:
            try:
                task = self.job_queue.get(timeout=30)
                self.process(task)
            except queue.Empty:
                return

    @staticmethod
    def transfer_file(kafka_message):
        s3 = boto3.client('s3')
        try:
            image_response = requests.get(kafka_message['url'], stream=True).raw
            s3.upload_fileobj(
                image_response,
                kafka_message['upload_bucket'],
                f"{kafka_message['file_path']}/{kafka_message['file_name']}")
        except FileNotFoundError:
            print("The file was not found")
        except NoCredentialsError:
            print("Credentials not available")

    def process(self, kafka_message):
        try:
            self.transfer_file(kafka_message)
        except Exception as err:
            print(traceback.format_exc())
            self.logger.log_to_errors(kafka_message['url'], str(err), traceback.format_exc(), 1)
