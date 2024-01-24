import json
import os


class SearchQueries:
    def __init__(self):
        queries = open('search/queries.json')
        self.queries = json.load(queries)

    def get(self, query_type):
        try:
            return self.queries[query_type]
        except Exception:
            raise
