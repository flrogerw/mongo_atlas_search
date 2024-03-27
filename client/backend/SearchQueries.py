import json
import yaml
import os
from nlp.StanzaNLP import StanzaNLP
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv

load_dotenv()
# ALLOWED_ENTITIES = ['podcast', 'station', 'episode']
SEARCHABLE_ENTITIES = ['podcast', 'station']
model = SentenceTransformer(os.getenv('VECTOR_MODEL_NAME'))


class SearchQueries:
    def __init__(self, LANGUAGES):
        self.nlp = StanzaNLP(LANGUAGES)
        self.primary = self.load_query('queries/primary.yml')
        self.union = self.load_query('queries/union.yml')

    def load_query(self, query_file):
        with open(query_file, "r") as f:
            return f.read()

    def build_query(self, search_phrase, max_results, ent_type, language):
        search_phrase = self.nlp.clean_text(search_phrase)
        lemma_text = self.nlp.get_lemma(search_phrase, language)
        vector_str = self.nlp.get_vector(search_phrase, model)

        if ent_type == 'all':
            for entity in SEARCHABLE_ENTITIES:
                collection = f"{entity}_{language}"
                entity_id_field = f'{entity}_id'
        else:
            collection = f"{ent_type}_{language}"
            entity_id_field = f"{ent_type}_id"

        yaml_query = self.primary.format(max_results=max_results, entity_id_field=entity_id_field,
                                         ent_type=ent_type, collection=collection, lemma_text=lemma_text)
        yaml_dict = yaml.full_load(yaml_query)
        pipeline = []
        for query in yaml_dict:
            if hasattr(query, '$vectorSearch'):
                print('HERE')
                query['$vectorSearch']['queryVector'] = vector_str
            pipeline.append(query)
        print(pipeline)
        return pipeline
