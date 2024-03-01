import psycopg2
import psycopg2.extras
from psycopg2.extensions import AsIs
import traceback


class PostgresDb:
    def __init__(self, username, password, database, host, schema):
        self.cursor = None
        self.connection = None
        self.database = database
        self.username = username
        self.password = password
        self.schema = schema
        self.host = host

    def connect(self):
        try:
            self.connection = psycopg2.connect(host=self.host,
                                               user=self.username,
                                               dbname=self.database,
                                               password=self.password,
                                               options=f'-c search_path={self.schema}')
            self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        except Exception as e:
            print(traceback.format_exc())
            pass

    def truncate_table(self, table_name):
        try:
            self.cursor.execute("TRUNCATE TABLE {table} RESTART IDENTITY".format(table=table_name))
            self.connection.commit()
        except Exception:
            print(traceback.format_exc())
            pass

    def append_ingest_ids(self, entity_type, table_type, response):
        try:
            ingest_ids = dict()
            argument_string = str([(d['record_hash'], d[f"{entity_type}_uuid"]) for d in response]).strip('[]')
            self.cursor.execute(
                f"INSERT INTO {entity_type}_ingest (record_hash, {entity_type}_uuid) VALUES {argument_string} RETURNING record_hash,{entity_type}_ingest_id as {entity_type}_{table_type}_id")
            result_list_of_tuples = (self.cursor.fetchall())
            for x in result_list_of_tuples:
                ingest_ids[x['record_hash']] = x[f"{entity_type}_{table_type}_id"]
            self.connection.commit()
            id_field = f"{entity_type}_{table_type}_id"
            for r in response:
                r[id_field] = ingest_ids[r['record_hash']]
            return response
        except Exception:
            print(traceback.format_exc())
            self.connection.commit()
            pass

    def insert_many(self, table_name, list_of_dicts):
        try:
            columns = ', '.join(list_of_dicts[0].keys())
            print(columns)
            place_holders = ','.join(['%s'] * len(list_of_dicts[0].keys()))
            insert_tuples = (tuple(d.values()) for d in list_of_dicts)
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({place_holders})"
            print(psycopg2.extras.execute_batch(self.cursor, query, insert_tuples))
        except Exception:
            print(traceback.format_exc())
            pass
        finally:
            self.connection.commit()

    def select_search_fields(self, table_name, columns, lang, offset, limit):
        try:
            query = f"SELECT {columns} FROM {table_name} WHERE language='{lang}' ORDER BY {table_name}_id OFFSET {offset} LIMIT {limit};"
            self.cursor.execute(query)
            data = [dict(row) for row in self.cursor.fetchall()]
            return data
        except Exception:
            print(traceback.format_exc())
            pass

    def select_all(self, table_name, columns, limit=10000):
        try:
            query = f"SELECT {columns} FROM {table_name} LIMIT {limit};"
            self.cursor.execute(query)
            data = [dict(row) for row in self.cursor.fetchall()]
            return data
        except Exception:
            print(traceback.format_exc())
            pass

    def close_connection(self):
        self.connection.close()
