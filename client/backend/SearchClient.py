from SearchQueries import SearchQueries
import pymongo
from dotenv import load_dotenv
import os
import concurrent.futures
import re

load_dotenv()
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')
MONGO_USER = os.getenv('MONGO_USER')
MONGO_PASS = os.getenv('MONGO_PASS')
MONGO_HOST = os.getenv('MONGO_HOST')
MONGO_PORT = os.getenv('MONGO_PORT')
LANGUAGES = os.getenv('LANGUAGES').split(",")
ATLAS_DB = os.getenv('ATLAS_DB')
SEARCHABLE_ENTITIES = os.getenv('SEARCHABLE_ENTITIES').split(",")


class SearchClient:
    def __init__(self):
        self.queries = SearchQueries(LANGUAGES)
        self.client = pymongo.MongoClient(
            f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/?directConnection=true")

    @staticmethod
    def highlight(search_result, search_phrase):
        keywords = search_phrase.split()
        for result in search_result:
            replacement = lambda match: re.sub(r'([^\s]+)', r'[m]\1[/m]', match.group())
            result['title'] = re.sub("|".join(map(re.escape, keywords)), replacement, result['title'], flags=re.I)

    def do_topten(self, ent_type, language):
        try:
            collection, pipeline = self.queries.build_top_ten(ent_type, language)
            search_result = list(self.client[ATLAS_DB][collection].aggregate(pipeline))
            if len(search_result) > 0:
                self.clean_up_scores(search_result)
                self.clean_subtitle(search_result)
            return search_result
        except Exception:
            raise

    def do_search(self, search_phrase, max_results, ent_type, language, query_type):
        try:
            collection, pipeline = self.queries.build_query(search_phrase, max_results, ent_type, language, query_type)
            search_result = list(self.client[ATLAS_DB][collection].aggregate(pipeline))
            if len(search_result) > 0:
                search_result = self.merge_records(search_result)
                self.clean_up_scores(search_result)
                self.clean_subtitle(search_result)
            return search_result
        except Exception:
            raise

    def do_autocomplete(self, search_phrase, max_results, ent_type, language):
        try:
            collection, pipeline = self.queries.build_autocomplete(search_phrase, max_results, ent_type, language)
            search_result = list(self.client[ATLAS_DB][collection].aggregate(pipeline))
            if len(search_result) > 0:
                self.clean_up_scores(search_result)
            self.highlight(search_result, search_phrase)
            return search_result
        except Exception:
            raise

    def top_ten(self, ent_type, language):
        try:
            search_result = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(SEARCHABLE_ENTITIES)) as executor:
                future_result = {
                    executor.submit(self.do_topten, entity, language): entity for entity in SEARCHABLE_ENTITIES
                }
                for future in concurrent.futures.as_completed(future_result):
                    try:
                        sorted_list = sorted(future.result(), key=lambda x: x['score'], reverse=True)
                        search_result.extend(sorted_list[:10])
                    except Exception:
                        raise
            #sorted_list = sorted(search_result, key=lambda x: x['score'], reverse=True)
            return search_result
        except Exception:
            raise

    def search_as_you_type(self, search_phrase, language, ent_type=None, max_results=7):
        try:
            search_result = []
            searchable_entities = [ent_type] if ent_type and ent_type != 'all' else SEARCHABLE_ENTITIES
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(SEARCHABLE_ENTITIES)) as executor:
                future_result = {
                    executor.submit(self.do_autocomplete, search_phrase, max_results, entity, language): entity for
                    entity in searchable_entities}
                for future in concurrent.futures.as_completed(future_result):
                    try:
                        search_result.extend(future.result())
                    except Exception:
                        raise
            sorted_list = sorted(search_result, key=lambda x: x['score'], reverse=True)
            return sorted_list[:max_results]
        except Exception:
            raise

    def search(self, search_phrase, language='en', ent_type='all', max_results=20, query_type='b'):
        try:
            search_result = []
            if ent_type == 'all':
                with concurrent.futures.ThreadPoolExecutor(max_workers=len(SEARCHABLE_ENTITIES)) as executor:
                    future_result = {
                        executor.submit(self.do_search, search_phrase, max_results, entity, language,
                                        query_type): entity for entity in SEARCHABLE_ENTITIES}
                    for future in concurrent.futures.as_completed(future_result):
                        try:
                            results = future.result()
                            sorted_list = sorted(results, key=lambda x: x['score'], reverse=True)
                            search_result.extend(sorted_list)
                        except Exception:
                            raise
                    return search_result
            else:
                result = self.do_search(search_phrase, max_results, ent_type, language, query_type)
                sorted_list = sorted(result, key=lambda x: x['score'], reverse=True)
                return sorted_list
        except Exception:
            raise

    @staticmethod
    def clean_subtitle(search_results):
        for result in search_results:
            if 'sub_title' in result:
                result['sub_title'] = [x for x in result['sub_title'] if x is not None]
            if 'sub_title' not in result or len(result['sub_title']) == 0:
                result['sub_title'] = ['Audacy National']

    @staticmethod
    def merge_records(results):
        try:
            final_results = []
            double_results = {}
            for result in results:
                double_results.setdefault(result[f"{result['entity_type']}_id"], []).append(result)
            for i in double_results:
                if len(double_results[i]) > 1:
                    lexical, semantic = double_results[i]
                    lexical['score'] += semantic['score']
                    final_results.append(lexical)
                else:
                    final_results.append(double_results[i][0])
            return final_results
        except Exception:
            raise

    @staticmethod
    def clean_up_scores(results):
        del_keys = ['advanced_popularity', 'listen_score', 'aps_score', 'atlas_score', 'language']
        for result in results:
            for k in del_keys:
                result.pop(k, None)
