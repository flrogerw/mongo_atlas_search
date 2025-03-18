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
from typing import Dict, Any

# Disable SSL warnings
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

# Configure retry adapter for HTTP requests
adapter = HTTPAdapter(max_retries=2)

# Load environment variables
load_dotenv()
DEFAULT_IMAGE_PATH: str = os.getenv('DEFAULT_IMAGE_PATH', '')


class FileUploadConsumer(threading.Thread):
    """
    A consumer thread that processes file upload tasks from a queue.
    It downloads files from URLs and uploads them to an S3 bucket.
    """

    def __init__(
            self,
            jobs_q: queue.Queue,
            update_q: queue.Queue,
            errors_q: queue.Queue,
            thread_lock: threading.Lock,
            *args,
            **kwargs
    ) -> None:
        """
        Initialize the FileUploadConsumer.

        Args:
            jobs_q (queue.Queue): Queue containing file upload jobs.
            update_q (queue.Queue): Queue for updates (e.g., error messages).
            errors_q (queue.Queue): Queue for logging errors.
            thread_lock (threading.Lock): Thread lock for safe concurrent access.
        """
        super().__init__(*args, **kwargs)
        self.req_session = requests.Session()
        self.req_session.mount('http://', adapter)
        self.req_session.mount('https://', adapter)
        self.logger = ErrorLogger(thread_lock, errors_q, None, None)
        self.job_queue = jobs_q
        self.errors_q = errors_q
        self.update_q = update_q
        self.thread_lock = thread_lock
        self.s3 = boto3.client('s3')

    def run(self) -> None:
        """
        Continuously process tasks from the job queue until it's empty.
        """
        while True:
            try:
                task = self.job_queue.get(timeout=30)
                self.process(task)
            except queue.Empty:
                return

    def transfer_file(self, kafka_message: Dict[str, Any]) -> None:
        """
        Transfers a file from a given URL to an S3 bucket.

        Args:
            kafka_message (Dict[str, Any]): Dictionary containing file metadata and upload details.
        """
        try:
            file_url = kafka_message['url']
            bucket_name = kafka_message['upload_bucket']
            file_key = f"{kafka_message['file_path']}/{kafka_message['file_name']}"

            # Handling RSS XML files
            if kafka_message['mime_type'] == 'application/rss+xml':
                response = self.req_session.get(file_url, timeout=2.0, verify=False)
                if response.status_code == 200:
                    self.s3.put_object(
                        Body=response.text,
                        Bucket=bucket_name,
                        Key=file_key,
                        ContentType='application/rss+xml'
                    )
                else:
                    self.update_q.put({
                        'podcast_uuid': kafka_message['podcast_uuid'],
                        'rss_url': f"URL returned a {response.status_code} error",
                    })

            # Handling other file types
            else:
                response = self.req_session.get(file_url, stream=True)
                if response.status_code == 200:
                    self.s3.upload_fileobj(
                        response.raw,
                        bucket_name,
                        file_key,
                        ExtraArgs={'ContentType': kafka_message['mime_type']}
                    )
                else:
                    self.update_q.put({
                        'podcast_uuid': kafka_message['podcast_uuid'],
                        'image_url': DEFAULT_IMAGE_PATH
                    })
        except ConnectionError:
            self.logger.log_to_errors(file_url, "Network connection error during file transfer.",
                                      traceback.format_exc(), 500)
        except Exception as e:
            self.logger.log_to_errors(file_url, f"Unexpected error in file transfer: {e}", traceback.format_exc(), 520)

    def process(self, kafka_message: Dict[str, Any]) -> None:
        """
        Processes a single file upload task.

        Args:
            kafka_message (Dict[str, Any]): Dictionary containing file metadata and upload details.
        """
        try:
            self.transfer_file(kafka_message)
        except Exception as err:
            self.logger.log_to_errors(kafka_message['url'], str(err), traceback.format_exc(), 510)
