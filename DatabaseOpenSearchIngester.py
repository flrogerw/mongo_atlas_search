import os
import pickle
import threading
import queue
import traceback
import ast
from datetime import datetime
from typing import List, Dict, Any

from open_search.SearchClient import SearchClient
from dotenv import load_dotenv
from sql.PostgresDb import PostgresDb
from logger.Logger import ErrorLogger

# Load system environment variables
load_dotenv()
DB_USER: str = os.getenv('DB_USER', '')
DB_PASS: str = os.getenv('DB_PASS', '')
DB_DATABASE: str = os.getenv('DB_DATABASE', '')
DB_HOST: str = os.getenv('DB_HOST', '')
DB_SCHEMA: str = os.getenv('DB_SCHEMA', '')
MONGO_DATABASE_NAME: str = os.getenv('MONGO_DATABASE_NAME', '')
MONGO_USER: str = os.getenv('MONGO_USER', '')
MONGO_PASS: str = os.getenv('MONGO_PASS', '')
MONGO_HOST: str = os.getenv('MONGO_HOST', '')
MONGO_PORT: str = os.getenv('MONGO_PORT', '27017')
INGESTER_CONFIG: List[tuple] = ast.literal_eval(os.getenv('INGESTER_CONFIG', '[]'))
LIMIT: int = 1000

# Initialize threading lock, database connection, search client, and logger
thread_lock = threading.Lock()
db = PostgresDb(DB_USER, DB_PASS, DB_DATABASE, DB_HOST, DB_SCHEMA)
search_client = SearchClient()
errors_q: queue.Queue = queue.Queue()
logger = ErrorLogger(thread_lock, errors_q)


def transform_docs(index: str, docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Transforms document records for indexing into OpenSearch.

    Args:
        index (str): The OpenSearch index name.
        docs (List[Dict[str, Any]]): List of documents to be transformed.

    Returns:
        List[Dict[str, Any]]: Transformed documents formatted for OpenSearch.
    """
    try:
        transformed = []
        for doc in docs:
            doc['is_explicit'] = bool(doc.get('is_explicit', False))
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
        logger.log_to_errors('TRANSFORM_DOCS_ERROR', str(err), traceback.format_exc(), 1)
        return []


def send_to_opensearch(logger: ErrorLogger, docs: List[Dict[str, Any]], index: str) -> None:
    """
    Sends transformed documents to OpenSearch.

    Args:
        logger (ErrorLogger): Logger instance for error tracking.
        docs (List[Dict[str, Any]]): List of documents to insert into OpenSearch.
        index (str): OpenSearch index name.

    Returns:
        None
    """
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
    except Exception as e:
        logger.log_to_errors('SEND_TO_OPENSEARCH_ERROR', str(e), traceback.format_exc(), 1)


if __name__ == '__main__':
    """
    Main execution process for the OpenSearch ingester.
    Iterates through the ingester configuration, fetches data from PostgreSQL, and inserts it into OpenSearch.
    """
    try:
        print('OpenSearch Ingester Process Started')
        for job in INGESTER_CONFIG:
            start_time = datetime.now()
            try:
                entity, languages, has_ingest_table, index = job
                SEARCH_FIELDS = os.getenv(f"{entity.upper()}_SEARCH_FIELDS", "")
                db.connect()

                # Fetch data from PostgreSQL
                batches = db.select_mongo_batches(
                    f"{entity}_quality", SEARCH_FIELDS, LIMIT, entity, has_ingest_table, languages
                )

                total = 0
                for batch in batches:
                    for record in batch:
                        try:
                            # Convert stored vector from binary format if present
                            if 'description_vector' in record and record['description_vector']:
                                record['description_vector'] = pickle.loads(record['description_vector'])
                        except (pickle.PickleError, KeyError) as e:
                            logger.log_to_errors('VECTOR_DESERIALIZATION_ERROR', str(e), traceback.format_exc(), 1)
                    send_to_opensearch(logger, batch, index)
                    total += len(batch)
                    print(f"\r{total} processed", end=' ')

                db.close_connection()
                elapsed_time = datetime.now() - start_time
                print(
                    f"\n{entity}s {', '.join(languages)} finished with {total} records inserted into OpenSearch {index} in {elapsed_time}.")

            except Exception as e:
                logger.log_to_errors('OPENSEARCH_INGESTER_JOB_ERROR', str(e), traceback.format_exc(), 1)
    except Exception as err:
        logger.log_to_errors('OPENSEARCH_INGESTER_ERROR', str(err), traceback.format_exc(), 1)
