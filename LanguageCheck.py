import os
import threading
import queue
import traceback
import spacy
from dotenv import load_dotenv
from spacy_langdetect import LanguageDetector
from spacy.language import Language
from sql.PostgresDb import PostgresDb
from logger.Logger import ErrorLogger
from nlp.ProcessText import ProcessText

# Load System ENV VARS
load_dotenv()
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_HOST = os.getenv('DB_HOST')
DB_SCHEMA = os.getenv('DB_SCHEMA')
LANGUAGE_MODEL = os.getenv('LANGUAGE_MODEL')
LANGUAGES = os.getenv('LANGUAGES').split(",")

thread_lock = threading.Lock()
db_reader = PostgresDb(DB_USER, DB_PASS, DB_DATABASE, DB_HOST, DB_SCHEMA)
db_writer = PostgresDb(DB_USER, DB_PASS, DB_DATABASE, DB_HOST, DB_SCHEMA)

errors_q = queue.Queue()
logger = ErrorLogger(thread_lock, errors_q)


def get_lang_detector(nlp, name):
    return LanguageDetector()


# Load Language Model and Sentence Transformer
nlp = spacy.load(LANGUAGE_MODEL)
Language.factory("language_detector", func=get_lang_detector)
nlp.add_pipe('language_detector', last=True)


def move_record_to_purgatory(table_name_from, table_name_to, columns, field, val, reason_for_failure):
    try:
        db_writer.connect()
        rec = db_writer.select_one(table_name_from, columns, field, val)
        rec['index_status'] = 320
        rec['reason_for_failure'] = reason_for_failure
        db_writer.insert_one(table_name_to, rec)
        db_writer.delete(table_name_from, field, val)
        print('DONE')

    except Exception:
        raise
    finally:
        db_writer.close_connection()


if __name__ == '__main__':
    try:
        print('Language Verify Process Started')
        total = 0
        db_reader.connect()
        batches = db_reader.select_mongo_batches(f"podcast_quality", "podcast_quality_id, description_cleaned", 500,
                                                 'podcast', True, ['en'])
        for batch in batches:
            for record in batch:
                total += 1
                print("\r" + f"{str(total)} processed", end=' ')
                get_lang = ProcessText.get_language_from_model(record['description_cleaned'], nlp)
                lang, certainty = get_lang
                if lang not in LANGUAGES:
                    get_columns = ['podcast_quality_id as podcast_purgatory_id', 'description_selected', 'readability',
                                   'is_explicit', 'index_status', 'episode_count', 'is_deleted', 'advanced_popularity',
                                   'listen_score_global', 'title_cleaned', f"'{lang}' as language",
                                   'description_cleaned', 'original_url', 'rss_url']
                    move_record_to_purgatory('podcast_quality',
                                             'podcast_purgatory',
                                             get_columns,
                                             'podcast_quality_id',
                                             record['podcast_quality_id'],
                                             f"Language was marked as {','.join(LANGUAGES)} but may be {lang} with {certainty} certainty.")
                # print(lang, certainty, record['description_cleaned'])
        db_reader.close_connection()

    except Exception as err:
        print(traceback.format_exc())
        logger.log_to_errors('LANGUAGE_CHECK_ERROR', str(err), traceback.format_exc(), 1)
        pass
