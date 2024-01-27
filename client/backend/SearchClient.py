from opensearchpy import OpenSearch, helpers
from SearchQueries import SearchQueries
import os
from dotenv import load_dotenv
from ProcessText import ProcessText

load_dotenv()


class SearchClient:
    def __init__(self):
        self.queries = SearchQueries()
        self.client = OpenSearch(
            hosts=[os.getenv('SEARCH_INSTANCE')],
            http_compress=True,
            use_ssl=True,
            verify_certs=False,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
            timeout=30
        )

    def search_as_you_type(self, search_phrase, index, size=10):
        try:
            query = self.queries.get('search_as_you_type')
            # nlp_text = self.nlp(search_phrase)
            # query = query % (int(size), nlp_text.get_clean())
            # return self.client.search(body=query, index=index)
        except Exception:
            raise

    def search(self, search_phrase, index, size=10):
        try:
            nlp_text = ProcessText(search_phrase)
            query = self.queries.get('hybrid')
            query = query % (int(size), nlp_text.get_tokens(), nlp_text.get_clean(), "pGttTI0BSrmE-sZF8kK4")
            return self.client.search(body=query, index='podcast_en_*')
        except Exception:
            raise
