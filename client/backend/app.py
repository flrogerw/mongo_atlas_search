import os
import json
from flask import Flask, request, Response
from SearchClient import SearchClient
from dotenv import load_dotenv

load_dotenv()
LANGUAGES = os.getenv('LANGUAGES').split(",")


app = Flask(__name__)
client = SearchClient()


@app.route('/update_aps', methods=["POST"])
def update_aps():
    response = ''
    return json.dumps(response)


@app.route('/autocomplete', methods=["GET"])
def autocomplete():
    try:
        r = request.args
        search_phrase = r.get('search_phrase') if r.get('search_phrase') else ''
        lang = r.get('lang') if r.get('lang') else 'en'
        if len(search_phrase) < 2:
            raise Exception('Search Phrase must be at least 2 characters')
        elif lang not in LANGUAGES:
            raise Exception(f'{lang} is not a supported language')
        else:
            response = client.search_as_you_type(search_phrase, lang)
            return json.dumps(response)

    except Exception as err:
        return Response(
            json.dumps({"message": str(err)}),
            status=418,
        )


@app.route('/search', methods=["GET"])
def search():
    try:
        r = request.args
        search_phrase = r.get('search_phrase') if r.get('search_phrase') else ''
        lang = r.get('lang') if r.get('lang') else 'en'
        ent_type = r.get('ent_type') if r.get('ent_type') else 'all'
        max_results = r.get('max_results') if r.get('max_results') else 20
        query_type = r.get('qm') if r.get('qm') else 'b'

        if len(search_phrase) < 2:
            raise Exception('Search Phrase must be at least 2 characters')
        elif lang not in LANGUAGES:
            raise Exception(f'{lang} is not a supported language')
        elif query_type not in ['s','l','b']:
            raise Exception(f"{query_type} is not a supported search type of (s)emantic, (l)exical or (b)oth))")
        else:
            response = client.search(
                search_phrase,
                lang,
                ent_type,
                max_results,
                query_type)
            return json.dumps(response)
    except Exception as err:
        return Response(
            json.dumps({"message": str(err)}),
            status=418,
        )
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
