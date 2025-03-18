import os
import pickle
import threading
import queue
from datetime import datetime
import pymongo
import traceback
from dotenv import load_dotenv
from sql.PostgresDb import PostgresDb
from logger.Logger import ErrorLogger
import ast
from typing import List, Dict, Any

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
MONGO_INGESTER_CONFIG: List[tuple] = ast.literal_eval(os.getenv('INGESTER_CONFIG', '[]'))
LIMIT: int = 1000

# Initialize thread lock, database connections, and logger
thread_lock = threading.Lock()
db = PostgresDb(DB_USER, DB_PASS, DB_DATABASE, DB_HOST, DB_SCHEMA)

mongo_client = pymongo.MongoClient(
    f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/?directConnection=true"
)
mongo_db = mongo_client[MONGO_DATABASE_NAME]

errors_q: queue.Queue = queue.Queue()
logger = ErrorLogger(thread_lock, errors_q)

def send_to_mongo(logger: ErrorLogger, batch: List[Dict[str, Any]], collection: str) -> None:
    """
    Sends a batch of records to MongoDB after handling any errors.

    Args:
        logger (ErrorLogger): Logger instance for error tracking.
        batch (List[Dict[str, Any]]): List of records to insert into MongoDB.
        collection (str): MongoDB collection name.

    Returns:
        None
    """
    try:
        logger.connect()

        # Retrieve and clear error queue
        errors_list = list(errors_q.queue)
        errors_q.queue.clear()

        # Insert errors into the error log if any exist
        if errors_list:
            logger.insert_many('error_log', errors_list)

        logger.close_connection()

        # Insert batch into MongoDB collection
        if batch:
            mongo_collection = mongo_db[collection]
            mongo_collection.insert_many(batch)

    except Exception as e:
        logger.log_to_errors('SEND_TO_MONGO_ERROR', str(e), traceback.format_exc(), 1)

if __name__ == '__main__':
    """
    Main execution process for the MongoDB ingester.
    Iterates through the ingester configuration, fetches data from PostgreSQL, and inserts it into MongoDB.
    """
    try:
        print('Mongo Ingester Process Started')

        for job in MONGO_INGESTER_CONFIG:
            start_time = datetime.now()

            try:
                entity, languages, has_ingest_table, collection = job
                SEARCH_FIELDS = os.getenv(f"{entity.upper()}_SEARCH_FIELDS", "")

                db.connect()

                # Fetch data from PostgreSQL
                batches = db.select_mongo_batches(
                    "podcast_quality", SEARCH_FIELDS, LIMIT, 'podcast', True, ['en', 'es', 'ru']
                )

                total = 0
                for batch in [batches]:
                    for record in batch:
                        try:
                            # Convert stored vector from binary format
                            if record.get('description_vector'):
                                record['description_vector'] = pickle.loads(record['description_vector']).tolist()
                        except (pickle.PickleError, KeyError) as e:
                            logger.log_to_errors('VECTOR_DESERIALIZATION_ERROR', str(e), traceback.format_exc(), 1)

                    send_to_mongo(logger, batch, collection)
                    total += len(batch)
                    print(f"\r{str(total)} processed", end=' ')

                db.close_connection()

                elapsed_time = datetime.now() - start_time
                print(
                    f"\n{entity}s {', '.join(languages)} finished with {total} records inserted into "
                    f"the Mongo {collection} collection in {elapsed_time}."
                )

            except Exception as e:
                logger.log_to_errors('MONGO_INGESTER_JOB_ERROR', str(e), traceback.format_exc(), 1)

    except Exception as err:
        logger.log_to_errors('DATABASE_MONGO_INGESTER_ERROR', str(err), traceback.format_exc(), 1)