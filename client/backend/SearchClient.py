from SearchQueries import SearchQueries
import pymongo
from dotenv import load_dotenv
import os
from sentence_transformers import SentenceTransformer
from nlp.StanzaNLP import StanzaNLP

load_dotenv()
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')
MONGO_USER = os.getenv('MONGO_USER')
MONGO_PASS = os.getenv('MONGO_PASS')
MONGO_HOST = os.getenv('MONGO_HOST')
MONGO_PORT = os.getenv('MONGO_PORT')
LANGUAGES = os.getenv('LANGUAGES').split(",")
VECTOR_MODEL_NAME = os.getenv('VECTOR_MODEL_NAME')

model = SentenceTransformer(os.getenv('VECTOR_MODEL_NAME'))


class SearchClient:
    def __init__(self):
        self.nlp = StanzaNLP(LANGUAGES)
        self.queries = SearchQueries(LANGUAGES)
        self.client = pymongo.MongoClient(
            f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/?directConnection=true")

    def search_as_you_type(self, search_phrase, index, size=10):
        try:
            query = self.queries.get('search_as_you_type')
            # nlp_text = self.nlp(search_phrase)
            # query = query % (int(size), nlp_text.get_clean())
            # return self.client.search(body=query, index=index)
        except Exception:
            raise

    def search(self, search_phrase, language='en', ent_type='all', max_results=10):
        try:
            pipeline = self.queries.build_query(search_phrase, max_results, ent_type, language)
            search_result = self.client["atlas_search"]["podcast_en"].aggregate(pipeline)
            print(search_result)
            results = {}
            for result in search_result:
                results.setdefault(result['source'], []).append(result)
            return results
        except Exception:
            raise
