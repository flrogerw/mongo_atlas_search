import psycopg2
import psycopg2.extras
from psycopg2.errors import UniqueViolation
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

    def error_retry(self, entity_type, table_type, response):
        entries_to_return = []
        for entry in response:
            try:
                query = f"INSERT INTO {entity_type}_ingest (record_hash, {entity_type}_uuid) VALUES ('{entry['record_hash']}','{entry[entity_type + '_uuid']}') RETURNING {entity_type}_ingest_id as {entity_type}_{table_type}_id"
                self.cursor.execute(query)
                result = self.cursor.fetchone()
                entry[f"{entity_type}_{table_type}_id"] = result[f"{entity_type}_{table_type}_id"]
                entries_to_return.append(entry)
                self.connection.commit()
            except UniqueViolation:
                print('UniqueViolation', entry['podcast_uuid'])

                #self.insert_many(f"{entity_type}_quarantine", [entry])
                self.connection.commit()
                continue
        return entries_to_return

    def append_ingest_ids(self, entity_type, table_type, response):
        try:
            ingest_ids = dict()
            argument_string = str([(d['record_hash'], d[f"{entity_type}_uuid"]) for d in response]).strip('[]')
            self.cursor.execute(
                f"INSERT INTO {entity_type}_ingest (record_hash, {entity_type}_uuid) VALUES {argument_string} RETURNING record_hash,{entity_type}_ingest_id as {entity_type}_{table_type}_id")
            result_list_of_tuples = (self.cursor.fetchall())
            for x in result_list_of_tuples:
                ingest_ids[x['record_hash']] = x[f"{entity_type}_{table_type}_id"]
            for r in response:
                r[f"{entity_type}_{table_type}_id"] = ingest_ids[r['record_hash']]
            return response
        except UniqueViolation:
            raise ValueError(response, entity_type, table_type)
        except Exception:
            print(traceback.format_exc())
        finally:
            self.connection.commit()

    def insert_many(self, table_name, list_of_dicts):
        try:
            columns = ', '.join(list_of_dicts[0].keys())
            place_holders = ','.join(['%s'] * len(list_of_dicts[0].keys()))
            insert_tuples = (tuple(d.values()) for d in list_of_dicts)
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({place_holders})"
            psycopg2.extras.execute_batch(self.cursor, query, insert_tuples)
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
