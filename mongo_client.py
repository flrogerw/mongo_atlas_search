import pymongo
import traceback
from dotenv import load_dotenv
import os
import torch
from sentence_transformers import SentenceTransformer
from client.backend.nlp.StanzaNLP import StanzaNLP
from collections import defaultdict

load_dotenv()
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')
MONGO_USER = os.getenv('MONGO_USER')
MONGO_PASS = os.getenv('MONGO_PASS')
MONGO_HOST = os.getenv('MONGO_HOST')
MONGO_PORT = os.getenv('MONGO_PORT')
LANGUAGES = os.getenv('LANGUAGES').split(",")

# connect to your Atlas cluster
client = pymongo.MongoClient(
    f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/?directConnection=true")

model = SentenceTransformer(os.getenv('VECTOR_MODEL_NAME'))

text = 'wfan'
nlp = StanzaNLP(LANGUAGES)
query_text = nlp.get_vector(text, model)
query_text = query_text.tolist()
lemma_text = nlp.get_lemma(text, 'en').lower()
# query_text = get_vector(text)
# print(get_vector('right wing politics'))
try:
    pipeline = [

        {"$vectorSearch": {"index": "knn", "path": "description_vector", "queryVector": query_text, "numCandidates": 10,
                           "limit": 10}},
        {"$project": {"_id": 0, "podcast_id": 1, "title": 1, "description": 1,
                      "advanced_popularity": 1,
                      "score": {"$meta": "vectorSearchScore"},
                      "normalizedScore": 1,
                      "listen_score": 1}},
        {"$set": {"source": "podcast"}},
        {
            "$addFields": {
                "normalizedScore": {
                    "$divide": [
                        "$score", 100
                    ]
                }
            }
        },
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
                    {"$set": {"source": "podcast"}},
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

    search_result = client["atlas_search"]["podcast_en"].aggregate(pipeline)

    # print(list(result))
    results = {}
    dups = {}
    for c, i in enumerate(search_result):
        if i['source'] in results:
            results[i['source']].append(i)
            dups[i['source']].append((i[f"{i['source']}_id"], c))
        else:
            results[i['source']] = [i]
            dups[i['source']] = [(i[f"{i['source']}_id"], c)]
    groups = {}
    for key, *values in dups['podcast']:
        groups.setdefault(key, []).append(values)
    new = [(k, *zip(*v)) for k, v in groups.items()]
    print(new)

    for result in results:
        sorted_list = sorted(results[result], key=lambda x: x['score'], reverse=True)
        results[result] = sorted_list

    print(results)


except Exception:
    print(traceback.format_exc())
