import pymongo
import traceback
from dotenv import load_dotenv
import os
import torch
from sentence_transformers import SentenceTransformer
from nlp.StanzaNLP import StanzaNLP

load_dotenv()
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')
MONGO_USER = os.getenv('MONGO_USER')
MONGO_PASS = os.getenv('MONGO_PASS')
MONGO_HOST = os.getenv('MONGO_HOST')
MONGO_PORT = os.getenv('MONGO_PORT')

# connect to your Atlas cluster
client = pymongo.MongoClient(
    f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/?directConnection=true")

model = SentenceTransformer(os.getenv('VECTOR_MODEL_NAME'))


def get_vector(text):
    try:
        with torch.no_grad():
            vector = model.encode(text)
        return vector.tolist()
    except Exception:
        raise


text = 'right wing politics'
nlp = StanzaNLP()
lemma_text = nlp.get_lemma(text)
query_text = get_vector(text)
# print(get_vector('right wing politics'))
try:
    pipeline = [

        {"$vectorSearch": {"index": "knn", "path": "vector", "queryVector": query_text, "numCandidates": 10,
                           "limit": 10}},
        {"$project": {"_id": 0, "podcast_id": 1, "title": 1, "description": 1,
                      "advanced_popularity": 1,
                      "score": {"$meta": "vectorSearchScore"},
                      "listen_score": 1}},
        {"$set": {"source": "podcast_v"}},
        {"$limit": 10},
        {
            "$unionWith": {
                "coll": "podcast_en",
                "pipeline": [
                    {"$search": {"index": "lemma",
                                 "text": {"query": lemma_text, "path": ["title_lemma", "description_lemma"]}}},
                    {"$project": {"_id": 0, "podcast_id": 1, "title": 1, "description": 1,
                                  "score": {"$meta": "searchScore"}
                                  }},
                    {"$set": {"source": "podcast_l"}},
                    {"$limit": 10}
                ]
            }
        },
        {
            "$unionWith": {
                "coll": "station_en",
                "pipeline": [
                    {"$search": {"index": "lemma",
                                 "text": {"query": lemma_text, "score": {"boost": {"value": 3}},
                                          "path": ["title_lemma", "description_lemma"]}}},
                    {"$project": {"_id": 0, "station_id": 1, "title": 1, "description": 1,
                                  "score": {"$meta": "searchScore"}}},
                    {"$set": {"source": "station"}},
                    {"$limit": 10}
                ]
            }
        },
        {
            "$sort": {
                "score": -1
            }
        }
    ]

    result = client["atlas_search"]["podcast_en"].aggregate(pipeline)

    # print(list(result))
    for i in result:
        print(i)
except Exception:
    print(traceback.format_exc())
