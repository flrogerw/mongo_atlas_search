from sql.PostgresDb import PostgresDb
from search.SearchClient import SearchClient
from dotenv import load_dotenv
import os
import pickle
import uuid
from datetime import datetime

load_dotenv()
SEARCH_FIELDS = os.getenv('SEARCH_FIELDS')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_HOST = os.getenv('DB_HOST')


def populate_search_queries():
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


def populate_search_titles(lang):
    transformed = []
    index = "search_titles"
    db = PostgresDb(DB_USER, DB_PASS, DB_DATABASE, DB_HOST)
    docs = db.select_search_fields('completed', ['title_cleaned', 'podcast_uuid'], lang)
    for doc in docs:
        transformed.append({
            '_op_type': 'index',
            '_index': index,
            '_id': doc['podcast_uuid'],
            '_source': doc
        })
    return transformed


def populate_podcasts(docs, lang):
    try:
        transformed = []
        index = "podcasts_{}".format(lang)
        for doc in docs:
            doc['vector'] = pickle.loads(doc['vector'])
            doc['is_explicit'] = bool(doc['is_explicit'])
            doc['is_deleted'] = bool(doc['is_deleted'])
            transformed.append({
                '_op_type': 'index',
                '_index': index,
                '_id': doc['podcast_uuid'],
                '_source': doc
            })
        return transformed
    except Exception as err:
        print(err)
    finally:
        db.close_connection()


if __name__ == '__main__':
    start_time = datetime.now()
    language = 'en'
    db = PostgresDb(DB_USER, DB_PASS, DB_DATABASE, DB_HOST)
    db.connect()


    offset = 0
    limit = 5000
    search_index = f'podcast_{language}'
    try:
        while True:
            #search_index = 'search_title'
            search_index = f'podcast_{language}'
            docs = db.select_search_fields('active', SEARCH_FIELDS, language, 0, 5000)
            transformed_data = populate_podcasts(language)
            #transformed_data = populate_search_titles(language)
            # print(transformed_data)
            sc = SearchClient()
            sc.post_to_search(transformed_data, search_index)
            print(datetime.now() - start_time)
    except Exception:
        raise
    finally:
        db.close_connection()