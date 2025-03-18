from sql.PostgresDb import PostgresDb
from open_search.SearchClient import SearchClient
from opensearchpy.exceptions import ConnectionTimeout
from dotenv import load_dotenv
import os
import pickle
import uuid
import threading
import queue
from typing import List, Dict, Any, Optional

# Load environment variables
load_dotenv()
SEARCH_FIELDS = os.getenv('SEARCH_FIELDS')


class SendToSearch(threading.Thread):
    """
    A threaded worker that processes jobs by sending data to OpenSearch.

    This worker retrieves jobs from a queue, processes them, and sends transformed
    documents to OpenSearch for indexing.
    """

    def __init__(
            self,
            index: str,
            jobs_q: queue.Queue,
            errors_q: queue.Queue,
            thread_lock: threading.Lock,
            *args,
            **kwargs
    ) -> None:
        """
        Initializes the SendToSearch thread.

        Args:
            index (str): The OpenSearch index to which documents will be sent.
            jobs_q (queue.Queue): The queue holding jobs to process.
            errors_q (queue.Queue): The queue for logging errors.
            thread_lock (threading.Lock): A threading lock for thread-safe operations.
        """
        super().__init__(*args, **kwargs)
        self.index: str = index
        self.jobs_q: queue.Queue = jobs_q
        self.errors_q: queue.Queue = errors_q
        self.thread_lock: threading.Lock = thread_lock
        self.search_client: SearchClient = SearchClient()

    def run(self) -> None:
        """
        Main loop that continuously processes jobs from the queue.

        Exits when the queue is empty for a certain period.
        """
        while True:
            try:
                job = self.jobs_q.get(timeout=5)  # Avoid infinite blocking
                self.process(job)
                self.jobs_q.task_done()
            except queue.Empty:
                print(f"[{self.index}] Job queue is empty, exiting thread.")
                return
            except Exception as e:
                self.log_error("PROCESS_ERROR", str(e))

    @staticmethod
    def populate_search_queries() -> List[Dict[str, Any]]:
        """
        Reads search queries from a file and prepares them for OpenSearch indexing.

        Returns:
            List[Dict[str, Any]]: A list of transformed documents ready for OpenSearch.
        """
        transformed: List[Dict[str, Any]] = []
        try:
            with open("seeds/podcast_searches.txt", "r", encoding="utf-8") as file:
                for line in file:
                    transformed.append({
                        '_op_type': 'index',
                        '_index': 'search_queries',
                        '_id': str(uuid.uuid4()),
                        '_source': {"search_query": line.strip()}
                    })
        except FileNotFoundError:
            print("Error: podcast_searches.txt not found.")
        except Exception as e:
            print(f"Error reading search queries: {e}")
        return transformed

    @staticmethod
    def populate_search_titles(lang: str) -> List[Dict[str, Any]]:
        """
        Retrieves and formats podcast search titles from a database.

        Args:
            lang (str): The language filter.

        Returns:
            List[Dict[str, Any]]: A list of transformed documents.
        """
        transformed: List[Dict[str, Any]] = []
        index = "search_titles"
        docs: List[Dict[str, Any]] = []  # Replace with actual DB query

        for doc in docs:
            transformed.append({
                '_op_type': 'index',
                '_index': index,
                '_id': doc.get('podcast_uuid', str(uuid.uuid4())),
                '_source': doc
            })
        return transformed

    def populate_podcasts(self, docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Prepares podcast documents for indexing in OpenSearch.

        Args:
            docs (List[Dict[str, Any]]): A list of podcast documents from the database.

        Returns:
            List[Dict[str, Any]]: Transformed documents ready for indexing.
        """
        try:
            transformed: List[Dict[str, Any]] = []
            for doc in docs:
                try:
                    # Deserialize vector data if present
                    doc['vector'] = pickle.loads(doc['vector']) if doc.get('vector') else None
                    doc['is_explicit'] = bool(doc.get('is_explicit', False))
                    doc['is_deleted'] = bool(doc.get('is_deleted', False))
                    transformed.append({
                        '_op_type': 'index',
                        '_index': self.index,
                        '_id': doc.get('podcast_uuid', str(uuid.uuid4())),
                        '_source': doc
                    })
                except (pickle.UnpicklingError, TypeError) as e:
                    self.log_error("PICKLE_ERROR", f"Error unpickling vector: {e}")
            return transformed
        except Exception as err:
            self.log_error("POPULATE_PODCASTS_ERROR", str(err))
            return []

    def process(self, job: List[Dict[str, Any]]) -> None:
        """
        Processes a single job by transforming and sending it to OpenSearch.

        Args:
            job (List[Dict[str, Any]]): A list of documents to process.
        """
        try:
            transformed_data = self.populate_podcasts(job)
            if transformed_data:
                self.search_client.post_to_search(transformed_data, self.index)
        except ConnectionTimeout:
            print(f"[{self.index}] ConnectionTimeout, re-queueing job.")
            self.jobs_q.put(job)
        except Exception as e:
            self.log_error("PROCESS_ERROR", str(e))

    def log_error(self, error_type: str, error_message: str) -> None:
        """
        Logs errors into the errors queue with thread safety.

        Args:
            error_type (str): The type of error.
            error_message (str): The error message.
        """
        with self.thread_lock:
            self.errors_q.put({
                "error_type": error_type,
                "message": error_message
            })
