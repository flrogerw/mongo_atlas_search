from SearchQueries import SearchQueries
import pymongo
from dotenv import load_dotenv
import os
import concurrent.futures
import re
from typing import List, Dict, Any, Optional, Tuple

load_dotenv()
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')
MONGO_USER = os.getenv('MONGO_USER')
MONGO_PASS = os.getenv('MONGO_PASS')
MONGO_HOST = os.getenv('MONGO_HOST')
MONGO_PORT = os.getenv('MONGO_PORT')
LANGUAGES = os.getenv('LANGUAGES', '').split(",")
ATLAS_DB = os.getenv('ATLAS_DB')
SEARCHABLE_ENTITIES = os.getenv('SEARCHABLE_ENTITIES', '').split(",")


class SearchClient:
    """
    A client for performing search queries on a MongoDB database, supporting tuning,
    autocomplete, and multi-threaded search operations.
    """

    def __init__(self) -> None:
        """
        Initializes the SearchClient and connects to MongoDB.
        """
        self.queries = SearchQueries(LANGUAGES)
        self.client = pymongo.MongoClient(
            f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/?directConnection=true"
        )

    @staticmethod
    def highlight(search_result: List[Dict[str, Any]], search_phrase: str) -> None:
        """
        Highlights keywords in search results.

        Args:
            search_result (List[Dict[str, Any]]): List of search results.
            search_phrase (str): Search phrase to highlight.
        """
        keywords = search_phrase.split()
        for result in search_result:
            replacement = lambda match: re.sub(r'([^\s]+)', r'[m]\1[/m]', match.group())
            result['title'] = re.sub("|".join(map(re.escape, keywords)), replacement, result['title'], flags=re.I)

    def do_search(self, search_phrase: str, ent_type: str, max_results: int, language: str, query_type: str = 'all') -> \
    List[Dict[str, Any]]:
        """
        Performs a search query in the database.

        Args:
            search_phrase (str): The search query.
            ent_type (str): The entity type to search.
            max_results (int): Maximum number of results to retrieve.
            language (str): Language of the search.
            query_type (str, optional): Type of query. Defaults to 'all'.

        Returns:
            List[Dict[str, Any]]: List of search results.
        """
        try:
            collection, pipeline = self.queries.build_query(search_phrase, max_results, ent_type, language, query_type)
            search_result = list(self.client[ATLAS_DB][collection].aggregate(pipeline))
            if search_result:
                search_result = self.merge_records(search_result)
                self.clean_up_scores(search_result)
                self.clean_subtitle(search_result)
            return search_result
        except Exception as e:
            raise RuntimeError(f"Error executing search: {e}")

    @staticmethod
    def clean_subtitle(search_results: List[Dict[str, Any]]) -> None:
        """
        Cleans subtitle fields in search results.

        Args:
            search_results (List[Dict[str, Any]]): List of search results.
        """
        for result in search_results:
            if 'sub_title' in result:
                result['sub_title'] = [x for x in result['sub_title'] if x is not None]
            if 'sub_title' not in result or not result['sub_title']:
                result['sub_title'] = ['Audacy National']

    @staticmethod
    def merge_records(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Merges duplicate records by combining scores.

        Args:
            results (List[Dict[str, Any]]): List of search results.

        Returns:
            List[Dict[str, Any]]: Merged search results.
        """
        try:
            final_results = []
            double_results = {}
            for result in results:
                double_results.setdefault(result[f"{result['entity_type']}_id"], []).append(result)
            for key, value in double_results.items():
                if len(value) > 1:
                    lexical, semantic = value
                    lexical['score'] += semantic['score']
                    final_results.append(lexical)
                else:
                    final_results.append(value[0])
            return final_results
        except Exception as e:
            raise RuntimeError(f"Error merging records: {e}")

    @staticmethod
    def clean_up_scores(results: List[Dict[str, Any]]) -> None:
        """
        Removes unnecessary fields from search results.

        Args:
            results (List[Dict[str, Any]]): List of search results.
        """
        del_keys = ['advanced_popularity', 'listen_score', 'aps_score', 'atlas_score', 'language']
        for result in results:
            for k in del_keys:
                result.pop(k, None)