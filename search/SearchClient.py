from opensearchpy import OpenSearch, helpers
from search.SearchQueries import SearchQueries
import os
from dotenv import load_dotenv
# from nlp.ProcessText import ProcessText

load_dotenv()


class SearchClient:
    def __init__(self):
        self.queries = SearchQueries()
        # self.nlp = ProcessText
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
            #query = query % (int(size), nlp_text.get_clean())
            #return self.client.search(body=query, index=index)
        except Exception:
            raise

    def search(self, search_phrase, index, size=10):

        try:
            nlp_text = self.nlp(search_phrase)
            query = self.queries.get('hybrid')
            query = query % (int(size), nlp_text.get_clean(), nlp_text.get_clean(), "fIBC14wBlHP3GAUwDjYR")
            return self.client.search(body=query, index=index)
        except Exception:
            raise

    def post_to_search(self, docs, index):
        try:
            helpers.bulk(self.client, docs, index=index, raise_on_error=True, refresh=True)
            del docs
        except Exception:
            raise

    def create_index(self, index, mappings):
        try:
            res = self.client.indices.create(index=index, body=mappings, ignore=[400])
            print(res)
        except Exception:
            raise
