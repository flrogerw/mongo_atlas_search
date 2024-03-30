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
            search_result = list(self.client[ATLAS_DB][collection].aggregate(pipeline))
            self.merge_records(search_result)
            self.clean_up_scores(search_result)
            sorted_list = sorted(search_result, key=lambda x: x['score'], reverse=True)
            return sorted_list[:int(max_results)]
        except Exception:
            raise

    """
    Merge scores when both semantic and lexical matches appear in the result set.
    """

    def merge_records(self, results):
        try:
            double_results = {}
            max_score = max(result['atlas_score'] for result in results)
            for c, i in enumerate(results):
                normalized_score = i['normalized_score'] if hasattr(i, 'normalized_score') else (i['atlas_score'] / max_score)
                i['score'] = normalized_score + i['listen_score'] + i['aps_score']
                entity_id = i[f"{i['entity_type']}_id"]
                double_results.setdefault(entity_id, []).append(c)
                if len(double_results[entity_id]) > 1:
                    x, y = double_results[entity_id]
                    combined_atlas = (results[x]['atlas_score'] + results[y]['atlas_score'])
                    normalized_score = combined_atlas / max_score
                    results[x]['score'] = normalized_score + results[x]['listen_score'] + results[x]['aps_score']
                    del results[y]
        except Exception:
            raise
    def clean_up_scores(self, results):
        del_keys = ['max_score', 'advanced_popularity', 'listen_score', 'aps_score', 'atlas_score', 'normalized_score']
        for result in results:
            for k in del_keys:
                result.pop(k, None)
