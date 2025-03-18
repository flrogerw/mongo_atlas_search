import yaml
import os
from nlp.StanzaNLP import StanzaNLP
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv
from typing import List, Tuple, Union, Dict

# Load environment variables
load_dotenv()
VECTOR_MODEL_NAME: str = os.getenv('VECTOR_MODEL_NAME', '')
LEXICAL_ONLY: List[str] = os.getenv('LEXICAL_ONLY', '').split(',')
SHARED_INDEXES: List[str] = os.getenv('SHARED_INDEXES', '').split(',')

# Load the Sentence Transformer model
model = SentenceTransformer(VECTOR_MODEL_NAME)


class SearchQueries:
    """
    A class to construct various search queries, including vector-based (KNN),
    lexical, autocomplete, and top-ten queries.
    """

    def __init__(self, languages: List[str]) -> None:
        """
        Initialize the SearchQueries class with NLP processing and query templates.

        Args:
            languages (List[str]): List of languages supported by StanzaNLP.
        """
        self.nlp = StanzaNLP(languages)
        self.knn: str = self.load_query('queries/knn.yml')
        self.lexical: str = self.load_query('queries/lexical.yml')
        self.autocomplete: str = self.load_query('queries/autocomplete.yml')
        self.topten: str = self.load_query('queries/topten.yml')

    @staticmethod
    def load_query(query_file: str) -> str:
        """
        Load a YAML query file as a string.

        Args:
            query_file (str): Path to the query YAML file.

        Returns:
            str: Query string content.
        """
        try:
            with open(query_file, "r") as f:
                return f.read()
        except Exception as e:
            raise FileNotFoundError(f"Error loading query file {query_file}: {e}")

    def get_knn_query(self, max_results: int, ent_type: str, collection: str,
                      vector, primary: bool = True) -> Union[List[Dict], Dict]:
        """
        Construct a KNN (vector search) query.

        Args:
            max_results (int): Maximum number of results.
            ent_type (str): Entity type.
            collection (str): Collection name.
            vector: Search vector.
            primary (bool): Whether this is the primary query.

        Returns:
            Union[List[Dict], Dict]: Query dictionary.
        """
        try:
            query: str = self.knn.format(max_results=max_results, entity_id_field=f"{ent_type}_id", ent_type=ent_type)
            query_dict: Dict = yaml.safe_load(query)
            query_dict[0]['$vectorSearch']['queryVector'] = vector.tolist()
            return [{"$unionWith": {"coll": collection, "pipeline": query_dict}}] if not primary else query_dict
        except Exception as e:
            raise ValueError(f"Error building KNN query: {e}")

    def get_lexical_query(self, max_results: int, ent_type: str, collection: str,
                          lemma_text: str, language: str, primary: bool = True) -> Union[List[Dict], Dict]:
        """
        Construct a lexical search query.

        Args:
            max_results (int): Maximum number of results.
            ent_type (str): Entity type.
            collection (str): Collection name.
            lemma_text (str): Lemmatized search phrase.
            language (str): Language code.
            primary (bool): Whether this is the primary query.

        Returns:
            Union[List[Dict], Dict]: Query dictionary.
        """
        try:
            query_yaml: str = self.lexical.format(max_results=max_results, entity_id_field=f"{ent_type}_id",
                                                  ent_type=ent_type, collection=collection, lemma_text=lemma_text,
                                                  synonyms=f"{ent_type}_synonyms", language=language)
            query_dict: Dict = yaml.safe_load(query_yaml)
            return [{"$unionWith": {"coll": collection, "pipeline": query_dict}}] if not primary else query_dict
        except Exception as e:
            raise ValueError(f"Error building lexical query: {e}")

    def get_autocomplete_query(self, search_phrase: str, max_results: int, language: str,
                               ent_type: str, collection: str) -> Dict:
        """
        Construct an autocomplete search query.

        Args:
            search_phrase (str): Search phrase.
            max_results (int): Maximum results.
            language (str): Language code.
            ent_type (str): Entity type.
            collection (str): Collection name.

        Returns:
            Dict: Query dictionary.
        """
        try:
            query_yaml: str = self.autocomplete.format(max_results=max_results, language=language,
                                                       query_text=search_phrase, synonyms=f"{ent_type}_synonyms")
            return yaml.safe_load(query_yaml)
        except Exception as e:
            raise ValueError(f"Error building autocomplete query: {e}")

    def build_top_ten(self, ent_type: str, language: str = 'en') -> Tuple[str, List[Dict]]:
        """
        Build a top ten query.

        Args:
            ent_type (str): Entity type.
            language (str): Language code. Defaults to 'en'.

        Returns:
            Tuple[str, List[Dict]]: Collection name and query pipeline.
        """
        try:
            collection: str = ent_type if ent_type != 'podcast' else 'top_ten_podcasts'
            query_yaml: str = self.topten.format(ent_type=ent_type, language=language)
            pipeline: List[Dict] = yaml.safe_load(query_yaml)
            return collection, pipeline
        except Exception as e:
            raise ValueError(f"Error building top ten query: {e}")
