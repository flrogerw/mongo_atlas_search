import sys
import json
from flask import Flask, request
from SearchClient import SearchClient

# sys.path.append("..")

app = Flask(__name__)
client = SearchClient()


@app.route('/update_aps', methods=["POST"])
def update_aps():
    response = ''
    return json.dumps(response)


@app.route('/search_as_you_type', methods=["GET"])
def search_as_you_type():
    search_phrase = request.args.get('search_phrase')
    index = request.args.get('index')
    response = client.search_as_you_type(search_phrase, index, 10)
    return json.dumps(response["hits"]["hits"])


@app.route('/search', methods=["GET"])
def search():
    response = client.search(
        request.args.get('search_phrase'),
        request.args.get('lang'),
        request.args.get('ent_type'),
        request.args.get('max_results'))
    return json.dumps(response)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
