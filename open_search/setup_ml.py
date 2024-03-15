import requests
import time
from urllib3.exceptions import InsecureRequestWarning
import os
from dotenv import load_dotenv

load_dotenv()
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
HOST = os.getenv('SEARCH_INSTANCE')
MODEL_ID = None
MODEL_STATE = None
print(HOST)
upload_model = {
    "name": "huggingface/sentence-transformers/{}".format(os.getenv('VECTOR_MODEL_NAME')),
    "version": "1.0.1",
    "model_format": "TORCH_SCRIPT"
}

# Post internal Model
url = "{}/_plugins/_ml/models/_upload".format(HOST)
post_response = requests.post(url, json=upload_model, verify=False)
res = post_response.json()
print(res)

# Get Model Id
url = "{}/_plugins/_ml/tasks/{}".format(HOST, res["task_id"])
while MODEL_ID is None:
    print('Waiting for Model to complete registration process')
    time.sleep(5)
    get_model_id = requests.get(url, verify=False)
    res = get_model_id.json()
    if "model_id" in res:
        MODEL_ID = res["model_id"]

# Load Model
url = "{}/_plugins/_ml/models/{}/_load".format(HOST, MODEL_ID)
load_model = requests.post(url, verify=False)
res = load_model.json();

time.sleep(5)
# Wait for Model to be in COMPLETED state
url = "{}/_plugins/_ml/tasks/{}".format(HOST, res["task_id"])
while MODEL_STATE is None:
    time.sleep(5)
    get_model_state = requests.get(url, verify=False)
    res = get_model_state.json()
    print('Waiting for Model STATE to be COMPLETED.  Current STATE is {}'.format(res["state"]))
    if res["state"] == "COMPLETED":
        MODEL_STATE = res["state"]

# Create Neural Search pipeline
pipeline = {
    "description": "Audacy POC for neural search pipeline",
    "processors": [
        {
            "text_embedding": {
                "model_id": MODEL_ID,
                "field_map": {
                    "description_vector": "description_vector"
                }
            }
        }
    ]
}
# In the Future
multi_vector = {
    "description": "An example neural search pipeline",
    "processors": [
        {
            "text_embedding": {
                "model_id": MODEL_ID,
                "field_map": {
                    "description": "description_vector",
                    "name": "name_vector"
                }
            }
        }
    ]
}

url = "{}/_ingest/pipeline/neural_pipeline".format(HOST, MODEL_ID)
put_pipeline = requests.put(url, json=pipeline, verify=False)
print(put_pipeline.json())
print(MODEL_ID)
