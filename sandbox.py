import csv
import os
from nlp.StanzaNLP import StanzaNLP
from sql.PostgresDb import PostgresDb
from dotenv import load_dotenv


load_dotenv()
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_HOST = os.getenv('DB_HOST')
DB_SCHEMA = os.getenv('DB_SCHEMA')

db = PostgresDb(DB_USER, DB_PASS, DB_DATABASE, DB_HOST, DB_SCHEMA)

def read_csv(csv_file):
    reader = csv.DictReader(open(csv_file), delimiter='\t')
    return list(reader)


if __name__ == '__main__':
    try:
        nlp = StanzaNLP(['en','es','ru'])
        aps_rows = read_csv('archives/aps_20240413.tsv')
        db.connect()
        for item in aps_rows:
            if item['PROPERTY_TYPE'] == 'PODCAST':
                #db.update_one('station_quality', 'advanced_popularity', item['APS_RAW_SCORE'], 'call_sign', nlp.clean_text(item['PROPERTY_NAME']))
                title_cleaned = nlp.clean_text(item['PROPERTY_NAME'])
                title_lemma = nlp.get_lemma(title_cleaned, 'en')
                db.update_one('podcast_quality', 'advanced_popularity', item['APS_RAW_SCORE'], 'LOWER(title_cleaned)', title_cleaned.lower())
                print(item['PROPERTY_NAME'], item['APS_RAW_SCORE'])

    except Exception as e:
        print(e)
