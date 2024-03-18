import os
import ast
import pickle
import threading
import queue
from datetime import datetime
import pymongo
import traceback
from dotenv import load_dotenv
from sql.PostgresDb import PostgresDb
from logger.Logger import ErrorLogger
from bson.binary import Binary

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
MONGO_INGESTER_CONFIG = ast.literal_eval(os.getenv('MONGO_INGESTER_CONFIG'))
LIMIT = 1000

thread_lock = threading.Lock()
db = PostgresDb(DB_USER, DB_PASS, DB_DATABASE, DB_HOST, DB_SCHEMA)

mongo_client = pymongo.MongoClient(
    f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/?directConnection=true")
mongo_db = mongo_client[MONGO_DATABASE_NAME]

errors_q = queue.Queue()
logger = ErrorLogger(thread_lock, errors_q)

def send_to_mongo(logger, batch, collection):
    try:
        logger.connect()
        errors_list = list(errors_q.queue)
        errors_q.queue.clear()

        if errors_list:
            logger.insert_many('error_log', errors_list)
        logger.close_connection()
        if batch:
            mongo_collection = mongo_db[collection]
            mongo_collection.insert_many(batch)
    except Exception:
        raise


if __name__ == '__main__':
    try:
        print('Mongo Ingester Process Started')
        for job in MONGO_INGESTER_CONFIG:
            start_time = datetime.now()
            entity, languages, has_ingest_table, collection = job
            SEARCH_FIELDS = os.getenv(f"{entity.upper()}_SEARCH_FIELDS")
            db.connect()
            batches = db.select_mongo_batches(f"{entity}_quality", SEARCH_FIELDS, LIMIT, entity, has_ingest_table, languages)
            total = 0
            for batch in batches:
                for record in batch:
                    record['vector'] = pickle.loads(record['vector']).tolist()
                    print(record)
                send_to_mongo(db, batch, collection)
                total += len(batch)
                print("\r" + f"{str(total)} processed", end=' ')
            db.close_connection()
            elapsed_time = datetime.now() - start_time
            print(f"\n{entity}s {', '.join(languages)} has finished with {total} records inserted into the Mongo {collection} collection in {elapsed_time}.")
    except Exception as err:
        print(traceback.format_exc())
        logger.log_to_errors('DATABASE_MONGO_INGESTER_ERROR', str(err), traceback.format_exc(), 1)
        pass
