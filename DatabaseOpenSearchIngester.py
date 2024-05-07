import os
import pickle
import threading
import queue
from datetime import datetime

import traceback
from open_search.SearchClient import SearchClient
from dotenv import load_dotenv
from sql.PostgresDb import PostgresDb
from logger.Logger import ErrorLogger
import ast

# Load System ENV VARS
load_dotenv()
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_HOST = os.getenv('DB_HOST')
DB_SCHEMA = os.getenv('DB_SCHEMA')
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')
MONGO_USER = os.getenv('MONGO_USER')
MONGO_PASS = os.getenv('MONGO_PASS')
MONGO_HOST = os.getenv('MONGO_HOST')
MONGO_PORT = os.getenv('MONGO_PORT')
INGESTER_CONFIG = ast.literal_eval(os.getenv('INGESTER_CONFIG'))
LIMIT = 1000

thread_lock = threading.Lock()
db = PostgresDb(DB_USER, DB_PASS, DB_DATABASE, DB_HOST, DB_SCHEMA)
search_client = SearchClient()

errors_q = queue.Queue()
logger = ErrorLogger(thread_lock, errors_q)


def transform_docs(index, docs):
    try:
        transformed = []
        for doc in docs:
            doc['is_explicit'] = bool(doc['is_explicit'])
            doc['is_deleted'] = False
            _id = doc['_id']
            del doc['_id']
            transformed.append({
                '_op_type': 'index',
                '_index': index,
                '_id': _id,
                '_source': doc
            })
        return transformed
    except Exception as err:
        print(err)
    # finally:
    # db.close_connection()


def send_to_opensearch(logger, docs, index):
    try:
        logger.connect()
        errors_list = list(errors_q.queue)
        errors_q.queue.clear()

        if errors_list:
            logger.insert_many('error_log', errors_list)
        logger.close_connection()
        if docs:
            transformed_data = transform_docs(index, docs)
            search_client.post_to_search(transformed_data, index)
    except Exception:
        raise


if __name__ == '__main__':
    try:
        print('OpenSearch Ingester Process Started')
        for job in INGESTER_CONFIG:
            start_time = datetime.now()
            entity, languages, has_ingest_table, index = job
            SEARCH_FIELDS = os.getenv(f"{entity.upper()}_SEARCH_FIELDS")
            db.connect()
            batches = db.select_mongo_batches(f"{entity}_quality", SEARCH_FIELDS, LIMIT, entity, has_ingest_table, languages)
            #batches = db.select_mongo_batches("podcast_quality", SEARCH_FIELDS, LIMIT, 'podcast', has_ingest_table, languages)
            total = 0
            for batch in batches:
                for record in batch:  ##  MAKE THIS LOOP THE VECTOR_FIELDS ENVAR
                    # record['tags'] = ast.literal_eval(record['tags']) if record['tags'] else []
                    # record['markets'] = ast.literal_eval(record['markets']) if record['markets'] else []
                    # record['genres'] = ast.literal_eval(record['genres']) if record['genres'] else []
                    # record['stations'] = list(ast.literal_eval(record['stations'])) if 'stations' in record else []
                    if record['description_vector']:
                        record['description_vector'] = pickle.loads(record['description_vector'])
                send_to_opensearch(db, batch, index)
                total += len(batch)
                print("\r" + f"{str(total)} processed", end=' ')
            db.close_connection()
            elapsed_time = datetime.now() - start_time
            print(
                f"\n{entity}s {', '.join(languages)} has finished with {total} records inserted into the Mongo {index} collection in {elapsed_time}.")
    except Exception as err:
        print(traceback.format_exc())
        logger.log_to_errors('DATABASE_MONGO_INGESTER_ERROR', str(err), traceback.format_exc(), 1)
        pass
