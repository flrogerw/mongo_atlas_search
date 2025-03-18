import json


class SearchQueries:
    """
    A class to manage and retrieve predefined OpenSearch query templates.

    This class loads query templates from a JSON file and provides a method
    to retrieve specific query templates based on a given key.
    """

    def __init__(self, query_file: str = './open_search/queries.json') -> None:
        """
        Initializes the SearchQueries instance by loading query templates from a JSON file.

        Args:
            query_file (str, optional): Path to the JSON file containing query templates.
                                        Defaults to './open_search/queries.json'.

        Raises:
            FileNotFoundError: If the specified query file is not found.
            ValueError: If the JSON file contains invalid JSON data.
            RuntimeError: For any other unexpected errors during loading.
        """
        try:
            with open(query_file, 'r', encoding='utf-8') as file:
                self.queries: dict = json.load(file)  # Load query templates into a dictionary
        except FileNotFoundError:
            raise FileNotFoundError(f"Query file not found: {query_file}")
        except json.JSONDecodeError:
            raise ValueError(f"Error decoding JSON from file: {query_file}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error loading queries: {e}")

    def get(self, query_type: str) -> dict:
        """
        Retrieves a query template based on the specified type.

        Args:
            query_type (str): The key identifying the desired query template.

        Returns:
            dict: The query template if found.

        Raises:
            KeyError: If the query type is not found in the loaded queries.
        """
        try:
            return self.queries[query_type]
        except KeyError:
            raise KeyError(f"Query type '{query_type}' not found in the queries file.")
