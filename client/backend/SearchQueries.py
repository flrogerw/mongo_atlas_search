import yaml
import os
from nlp.StanzaNLP import StanzaNLP
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv

load_dotenv()
VECTOR_MODEL_NAME = os.getenv('VECTOR_MODEL_NAME')
LEXICAL_ONLY = os.getenv('LEXICAL_ONLY').split(',')
SHARED_INDEXES = os.getenv('SHARED_INDEXES').split(',')

model = SentenceTransformer(VECTOR_MODEL_NAME)


class SearchQueries:
    def __init__(self, languages):
        self.nlp = StanzaNLP(languages)
        self.knn = self.load_query('queries/knn.yml')
        self.lexical = self.load_query('queries/lexical.yml')
        self.autocomplete = self.load_query('queries/autocomplete.yml')

    @staticmethod
    def load_query(query_file):
        try:
            with open(query_file, "r") as f:
                return f.read()
        except Exception:
            raise

    def get_knn_query(self, max_results, ent_type, collection, vector, primary=True):
        try:
            query = self.knn.format(max_results=max_results, entity_id_field=f"{ent_type}_id",
                                    ent_type=ent_type)
            query_dict = yaml.safe_load(query)
            query_dict[0]['$vectorSearch']['queryVector'] = vector.tolist()
            return [{"$unionWith": {"coll": collection, "pipeline": query_dict}}] if not primary else query_dict
        except Exception:
            raise

    def get_lexical_query(self, max_results, ent_type, collection, lemma_text, language, primary=True):
        try:
            query_yaml = self.lexical.format(max_results=max_results, entity_id_field=f"{ent_type}_id",
                                             ent_type=ent_type, collection=collection, lemma_text=lemma_text,
                                             synonyms=f"{ent_type}_synonyms", language=language)
            query_dict = yaml.safe_load(query_yaml)
            return [{"$unionWith": {"coll": collection, "pipeline": query_dict}}] if not primary else query_dict
        except Exception:
            raise

    def get_autocomplete_query(self, search_phrase, max_results, language, ent_type, collection):
        try:
            query_yaml = self.autocomplete.format(max_results=max_results, language=language, query_text=search_phrase, synonyms=f"{ent_type}_synonyms")
            return yaml.safe_load(query_yaml)
        except Exception:
            raise

    def build_autocomplete(self, search_phrase, max_results, ent_type, language):
        pipeline = []
        collection = ent_type if ent_type in SHARED_INDEXES else f"{ent_type}_{language}"
        search_phrase = self.nlp.clean_text(search_phrase).lower()
        pipeline.extend(self.get_autocomplete_query(search_phrase, max_results, language, ent_type, collection))
        return collection, pipeline

    def build_query(self, search_phrase, max_results, ent_type, language, query_type):
        try:
            search_phrase = self.nlp.clean_text(search_phrase).lower()
            pipeline = []
            if ent_type in LEXICAL_ONLY:
                collection = ent_type
                lemma_text = self.nlp.get_lemma(search_phrase, language)
                pipeline.extend(self.get_lexical_query(max_results, ent_type, collection, lemma_text, language))
            else:
                collection = f"{ent_type}_{language}"
                if query_type in ['b', 's']:
                    vector = self.nlp.get_vector(search_phrase, model)
                    pipeline.extend(self.get_knn_query(max_results, ent_type, collection, vector, True))
                if query_type in ['b', 'l']:
                    primary = True if query_type == 'l' else False
                    lemma_text = self.nlp.get_lemma(search_phrase, language)
                    pipeline.extend(self.get_lexical_query(max_results, ent_type, collection, lemma_text, language, primary))
            return collection, pipeline
        except Exception:
            raise
