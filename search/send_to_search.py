from sql.SqliteDb import Db
from search.SearchClient import SearchClient
from dotenv import load_dotenv
import os
import pickle
import uuid

load_dotenv()
SEARCH_FIELDS = os.getenv('SEARCH_FIELDS').split(',')
VECTOR_FIELD = os.getenv('FIELD_TO_VECTOR') + '_vector'

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
    db = Db()
    docs = db.select_search_fields('completed', ['title_cleaned', 'podcast_uuid'], lang)
    for doc in docs:
        transformed.append({
            '_op_type': 'index',
            '_index': index,
            '_id': doc['podcast_uuid'],
            '_source': doc
        })
    return transformed


def populate_podcasts(lang):
    transformed = []
    index = "podcasts_{}".format(lang)
    db = Db()
    docs = db.select_search_fields('completed', SEARCH_FIELDS, lang)
    for doc in docs:
        doc[VECTOR_FIELD] = pickle.loads(doc[VECTOR_FIELD])
        doc['is_explicit'] = bool(doc['is_explicit'])
        doc['is_deleted'] = bool(doc['is_deleted'])
        transformed.append({
            '_op_type': 'index',
            '_index': index,
            '_id': doc['podcast_uuid'],
            '_source': doc
        })
    return transformed


if __name__ == '__main__':
    language = 'en'
    search_index = 'search_title'
    # transformed_data = populate_podcasts(language)
    transformed_data = populate_search_titles(language)
    # print(transformed_data)
    sc = SearchClient()
    sc.post_to_search(transformed_data, search_index)
