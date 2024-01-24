from sql.PostgresDb import PostgresDb
from search.SearchClient import SearchClient
from opensearchpy.exceptions import ConnectionTimeout
from dotenv import load_dotenv
import os
import pickle
import uuid
import threading
import queue

load_dotenv()
SEARCH_FIELDS = os.getenv('SEARCH_FIELDS')


class SendToSearch(threading.Thread):
    def __init__(self, index, jobs_q, errors_q, thread_lock, *args, **kwargs):
        self.index = index
        self.jobs_q = jobs_q
        self.search_client = SearchClient()

        super().__init__(*args, **kwargs)

    def run(self):
        while True:
            try:
                job = self.jobs_q.get()
                self.process(job)
            except queue.Empty:
                print('EMPTY')
                return
            self.jobs_q.task_done()

    def populate_search_queries(self):
        transformed = []
        with open("seeds/podcast_searches.txt", "r") as file:
            while line := file.readline():
                transformed.append({
                    '_op_type': 'index',
                    '_index': 'search_queries',
                    '_id': str(uuid.uuid4()),
                    '_source': {"search_query": line.rstrip()}
                })

        return transformed

    def populate_search_titles(self, lang):
        transformed = []
        index = "search_titles"
        # db = PostgresDb(DB_USER, DB_PASS, DB_DATABASE, DB_HOST)
        docs = []  # db.select_search_fields('active', ['title_cleaned', 'podcast_uuid'], lang)
        for doc in docs:
            transformed.append({
                '_op_type': 'index',
                '_index': index,
                '_id': doc['podcast_uuid'],
                '_source': doc
            })
        return transformed

    def populate_podcasts(self, docs):
        try:
            transformed = []
            for doc in docs:
                doc['vector'] = pickle.loads(doc['vector'])
                doc['is_explicit'] = bool(doc['is_explicit'])
                doc['is_deleted'] = bool(doc['is_deleted'])
                transformed.append({
                    '_op_type': 'index',
                    '_index': self.index,
                    '_id': doc['podcast_uuid'],
                    '_source': doc
                })
            return transformed
        except Exception as err:
            print(err)
        # finally:
        # db.close_connection()

    def process(self, job):
        try:
            transformed_data = self.populate_podcasts(job)
            # transformed_data = populate_search_titles(language)
            self.search_client.post_to_search(transformed_data, self.index)
        except ConnectionTimeout:
            print('ConnectionTimeout, job sent back to queue')
            self.jobs_q.put(job)
        except Exception as e:
            print(e)
            pass
