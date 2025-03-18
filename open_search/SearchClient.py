from opensearchpy import OpenSearch, helpers
from open_search.SearchQueries import SearchQueries
import os
from dotenv import load_dotenv
from nlp.StanzaNLP import StanzaNLP
from typing import List, Dict, Any

# Load environment variables
load_dotenv()
LANGUAGES = os.getenv('LANGUAGES', 'en').split(",")


class SearchClient:
    """
    A client to interact with an OpenSearch instance for searching and indexing documents.
    """

    def __init__(self):
        """
        Initializes the SearchClient with OpenSearch connection, query templates, and NLP processing.
        """
        try:
            self.queries = SearchQueries()
            self.nlp = StanzaNLP(LANGUAGES)
            self.client = OpenSearch(
                hosts=[os.getenv('SEARCH_INSTANCE')],
                http_compress=True,
                use_ssl=True,
                verify_certs=False,
                ssl_assert_hostname=False,
                ssl_show_warn=False,
                timeout=30
            )
        except Exception as e:
            print(f"Error initializing SearchClient: {e}")
            raise

    def search_as_you_type(self, search_phrase: str, index: str, size: int = 10) -> Dict[str, Any]:
        """
        Performs a "search as you type" query.

        Args:
            search_phrase (str): The phrase to search for.
            index (str): The OpenSearch index to query.
            size (int, optional): The number of results to return. Defaults to 10.

        Returns:
            dict: The OpenSearch search results.
        """
        try:
            query = self.queries.get('search_as_you_type')
            # Uncomment the following lines if NLP processing is required
            # nlp_text = self.nlp.clean_text(search_phrase)
            # query = query % (int(size), nlp_text)
            return self.client.search(body=query, index=index)
        except Exception as e:
            print(f"Error in search_as_you_type: {e}")
            raise

    def search(self, search_phrase: str, index: str, size: int = 10) -> Dict[str, Any]:
        """
        Performs a hybrid search using NLP-processed text.

        Args:
            search_phrase (str): The phrase to search for.
            index (str): The OpenSearch index to query.
            size (int, optional): The number of results to return. Defaults to 10.

        Returns:
            dict: The OpenSearch search results.
        """
        try:
            nlp_text = self.nlp.clean_text(search_phrase)  # Process the text with NLP
            query = self.queries.get('hybrid')
            query = query % (int(size), nlp_text, nlp_text, "fIBC14wBlHP3GAUwDjYR")
            return self.client.search(body=query, index=index)
        except Exception as e:
            print(f"Error in search: {e}")
            raise

    def post_to_search(self, docs: List[Dict[str, Any]], index: str) -> None:
        """
        Indexes a batch of documents into OpenSearch.

        Args:
            docs (list): List of documents to index.
            index (str): The OpenSearch index.

        Raises:
            Exception: If indexing fails.
        """
        try:
            helpers.bulk(self.client, docs, index=index, raise_on_error=True, refresh=True)
            del docs  # Free memory after indexing
        except Exception as e:
            print(f"Error in post_to_search: {e}")
            raise

    def create_index(self, index: str, mappings: Dict[str, Any]) -> None:
        """
        Creates a new OpenSearch index with specified mappings.

        Args:
            index (str): The name of the index.
            mappings (dict): The mappings configuration for the index.

        Raises:
            Exception: If index creation fails.
        """
        try:
            res = self.client.indices.create(index=index, body=mappings, ignore=[400])
            print(f"Index creation response: {res}")
        except Exception as e:
            print(f"Error in create_index: {e}")
            raise
