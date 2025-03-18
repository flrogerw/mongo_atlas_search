import os
import json
from flask_cors import CORS, cross_origin
from flask import Flask, request, Response
from SearchClient import SearchClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
LANGUAGES: list[str] = os.getenv('LANGUAGES', '').split(",")

# Initialize Flask app
app: Flask = Flask(__name__, static_url_path='', static_folder='web')
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'
client: SearchClient = SearchClient()


@app.route('/web')
def static_file() -> Response:
    """Serve the static index.html file."""
    return app.send_static_file('index.html')


@app.route('/update_aps', methods=["POST"])
@cross_origin()
def update_aps() -> str:
    """Endpoint placeholder for updating APS (not implemented)."""
    response: str = ''
    return json.dumps(response)


@app.route('/amperwave', methods=["POST"])
@cross_origin()
def amperwave() -> Response:
    """Handle incoming Amperwave requests."""
    try:
        print(request)
    except Exception as err:
        return Response(json.dumps({"message": str(err)}), status=418)


@app.route('/autocomplete', methods=["GET"])
@cross_origin()
def autocomplete() -> Response:
    """Autocomplete suggestions based on search phrase and language."""
    try:
        r = request.args
        search_phrase: str = r.get('search_phrase', '')
        lang: str = r.get('lang', 'en')
        ent_type: str | None = r.get('ent_type')

        if len(search_phrase) < 2:
            raise ValueError('Search Phrase must be at least 2 characters')
        if lang not in LANGUAGES:
            raise ValueError(f'{lang} is not a supported language')

        response = client.search_as_you_type(search_phrase, lang, ent_type)
        return Response(json.dumps(response), status=200)
    except Exception as err:
        return Response(json.dumps({"message": str(err)}), status=418)


@app.route('/tuning', methods=["GET"])
@cross_origin()
def tuning() -> Response:
    """Fetch tuning configurations."""
    try:
        response = client.tuning(request.args)
        return Response(json.dumps(response), status=200)
    except Exception as err:
        return Response(json.dumps({"message": str(err)}), status=418)


@app.route('/topten', methods=["GET"])
@cross_origin()
def topten() -> Response:
    """Retrieve top ten search results for a given language and entity type."""
    try:
        r = request.args
        lang: str = r.get('lang', 'en')
        ent_type: str = r.get('ent_type', 'all')
        response = client.top_ten(ent_type, lang)
        return Response(json.dumps(response), status=200)
    except Exception as err:
        return Response(json.dumps({"message": str(err)}), status=418)


@app.route('/search', methods=["GET"])
@cross_origin()
def search() -> Response:
    """Perform a search query with various parameters."""
    try:
        r = request.args
        search_phrase: str = r.get('search_phrase', '')
        lang: str = r.get('lang', 'en')
        ent_type: str = r.get('ent_type', 'all')
        max_results: int = int(r.get('max_results', 10))
        query_type: str = r.get('qm', 'b')

        if len(search_phrase) < 2:
            raise ValueError('Search Phrase must be at least 2 characters')
        if lang not in LANGUAGES:
            raise ValueError(f'{lang} is not a supported language')
        if query_type not in ['s', 'l', 'b']:
            raise ValueError(f"{query_type} is not a supported search type (s)emantic, (l)exical, or (b)oth")

        response = client.search(search_phrase, lang, ent_type, max_results, query_type)
        return Response(json.dumps(response), status=200)
    except Exception as err:
        return Response(json.dumps({"message": str(err)}), status=418)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
