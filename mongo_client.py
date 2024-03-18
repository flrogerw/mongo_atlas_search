import pymongo
import traceback
from dotenv import load_dotenv
import os
import torch
from sentence_transformers import SentenceTransformer

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

text='wfan'
query_text = get_vector(text)
# print(get_vector('right wing politics'))
try:
    pipeline = [
        {
            "$vectorSearch": {
                "index": "semantic",
                "path": "vector",
                "queryVector": query_text,
                "numCandidates": 10,
                "limit": 10
            }
        }, {
            '$project': {
                'score': {'$meta': 'searchScore'}, '_id': 0, 'podcast_id': 1, 'title': 1, 'description': 1}
        }, {
            '$set': {'source': 'podcast'}
        }, {
            '$limit': 20
        }, {
            '$unionWith': {
                'coll': 'station_en',
                'pipeline': [
                    {
                        '$search': {
                            'text': {'query': text, 'path': ['title_lemma','description_lemma']}
                        }
                    },
                    {
                        '$set': {'source': 'station'}
                    }, {
                        '$project': {
                             'score': {'$meta': 'searchScore'}, '_id': 0, 'title': 1, 'description': 1}
                    }, {
                        '$limit': 10
                    }, {
                        '$sort': {'score': 1}
                    }
                ]
            }
        }
    ]


    result = client["atlas_search"]["podcast_en"].aggregate(pipeline)

    # print(list(result))
    for i in result:
        print(i)
    print(i['title'], "\n", i['description'], "\n")
except Exception:
    print(traceback.format_exc())
