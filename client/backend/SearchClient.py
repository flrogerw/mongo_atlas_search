from SearchQueries import SearchQueries
import pymongo
from dotenv import load_dotenv
import os

load_dotenv()
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')
MONGO_USER = os.getenv('MONGO_USER')
MONGO_PASS = os.getenv('MONGO_PASS')
MONGO_HOST = os.getenv('MONGO_HOST')
MONGO_PORT = os.getenv('MONGO_PORT')
LANGUAGES = os.getenv('LANGUAGES').split(",")
ATLAS_DB = os.getenv('ATLAS_DB')


class SearchClient:
    def __init__(self):
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
            collection, pipeline = self.queries.build_query(search_phrase, max_results, ent_type, language)
            search_result = self.client[ATLAS_DB][collection].aggregate(pipeline)
            results = {}
            for i in search_result:
                if i['entity_type'] in results:
                    results[i['entity_type']].append(i)
                else:
                    results[i['entity_type']] = [i]
            for result in results:
                sorted_list = sorted(results[result], key=lambda x: x['score'], reverse=True)
                results[result] = sorted_list
            return results
        except Exception:
            raise
