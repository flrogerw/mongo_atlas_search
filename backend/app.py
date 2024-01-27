import sys
import json
from flask import Flask, request
from SearchClient import SearchClient
#sys.path.append("..")

app = Flask(__name__)
#load_dotenv()


@app.route('/update_aps', methods=["POST"])
def update_aps():
    client = SearchClient()
    # response = client.search(body=query, index=index_name, params=params)
    # return json.dumps(response["hits"]["hits"])


@app.route('/search_as_you_type', methods=["GET"])
def search_as_you_type():
    client = SearchClient()
    search_phrase = request.args.get('search_phrase')
    index = request.args.get('index')
    response = client.search_as_you_type(search_phrase, index, 10)
    return json.dumps(response["hits"]["hits"])


@app.route('/search', methods=["GET"])
def search():
    client = SearchClient()
    search_phrase = request.args.get('search_phrase')
    index = "%s_%s" % (request.args.get('entity'), request.args.get('language'))
    size = request.args.get('size')
    response = client.search(search_phrase, index, size)
    return json.dumps(response["hits"]["hits"])


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
