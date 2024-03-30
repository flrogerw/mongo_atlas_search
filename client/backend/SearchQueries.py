import yaml
import os
from nlp.StanzaNLP import StanzaNLP
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv

load_dotenv()
SEARCHABLE_ENTITIES = os.getenv('SEARCHABLE_ENTITIES').split(",")
VECTOR_MODEL_NAME = os.getenv('VECTOR_MODEL_NAME')

model = SentenceTransformer(VECTOR_MODEL_NAME)


class SearchQueries:
    def __init__(self, languages):
        self.nlp = StanzaNLP(languages)
        self.knn = self.load_query('queries/knn.yml')
        self.lexical = self.load_query('queries/lexical.yml')

    def load_query(self, query_file):
        with open(query_file, "r") as f:
            return f.read()

    def get_knn_query(self, max_results, ent_type, collection, vector, primary=True):
        query = self.knn.format(max_results=max_results, entity_id_field=f"{ent_type}_id",
                                ent_type=ent_type)
        query_dict = yaml.safe_load(query)
        query_dict[0]['$vectorSearch']['queryVector'] = vector.tolist()
        return [{"$unionWith": {"coll": collection, "pipeline": query_dict}}] if not primary else query_dict

    def get_lexical_query(self, max_results, ent_type, collection, lemma_text, primary=True):
        query_yaml = self.lexical.format(max_results=max_results, entity_id_field=f"{ent_type}_id",
                                         ent_type=ent_type, collection=collection, lemma_text=lemma_text)
        query_dict = yaml.safe_load(query_yaml)
        return [{"$unionWith": {"coll": collection, "pipeline": query_dict}}] if not primary else query_dict

    def build_query(self, search_phrase, max_results, ent_type, language):
        search_phrase = self.nlp.clean_text(search_phrase).lower()
        print(search_phrase)
        lemma_text = self.nlp.get_lemma(search_phrase, language)
        vector = self.nlp.get_vector(search_phrase, model)
        pipeline = []
        search_collection = None
        if ent_type == 'all':
            is_primary = True
            for entity in SEARCHABLE_ENTITIES:
                collection = f"{entity}_{language}"
                search_collection = collection if is_primary else search_collection
                ent_type = entity
                pipeline.extend(self.get_knn_query(max_results, ent_type, collection, vector, is_primary))
                is_primary = False
                pipeline.extend(self.get_lexical_query(max_results, ent_type, collection, lemma_text, is_primary))
        else:
            search_collection = collection = f"{ent_type}_{language}"
            pipeline.extend(self.get_knn_query(max_results, ent_type, collection, vector, True))
            pipeline.extend(self.get_lexical_query(max_results, ent_type, collection, lemma_text, False))
        return search_collection, pipeline
