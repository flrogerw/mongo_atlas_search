import psycopg2
import psycopg2.extras
from psycopg2.errors import UniqueViolation
import traceback
import itertools


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
                print('UniqueViolation')
                self.connection.commit()
                columns = f"{entity_type}_uuid,original_{entity_type}_uuid,duplicate_file_name"
                values = (entry[f"{entity_type}_uuid"], entry[f"{entity_type}_uuid"], 'DB INSERT DUPLICATE')
                try:
                    query = f"INSERT INTO {entity_type}_quarantine ({columns}) VALUES {values}"
                    self.cursor.execute(query)
                    self.connection.commit()
                except Exception:
                    raise
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

    def select_one(self, table_name, columns, field, val):
        try:
            cols = ','.join(columns)
            query = f"SELECT {cols} FROM {table_name} WHERE {field} = {val};"
            self.cursor.execute(query)
            return dict(self.cursor.fetchone())
        except Exception:
            print(traceback.format_exc())
            pass
        finally:
            self.connection.commit()

    def upsert_one(self, table_name, record, conflict):
        try:
            columns = ','.join(record.keys())
            #query = f"INSERT INTO {table_name} ({columns}) VALUES {tuple(record.values())} ON CONFLICT({conflict}) DO UPDATE;"
            #self.cursor.execute(query)
        except Exception:
            print(traceback.format_exc())
            pass
        finally:
            self.connection.commit()

    def insert_one(self, table_name, record):
        try:
            columns = ','.join(record.keys())
            query = f"INSERT INTO {table_name} ({columns}) VALUES {tuple(record.values())};"
            self.cursor.execute(query)
        except Exception:
            print(traceback.format_exc())
            pass
        finally:
            self.connection.commit()

    def select_all(self, table_name, columns, limit=10000):
        try:
            query = f"SELECT {columns} FROM {table_name} LIMIT {limit};"
            self.cursor.execute(query)
            data = [dict(row) for row in self.cursor.fetchall()]
            return data
        except Exception:
            print(traceback.format_exc())
            pass
        finally:
            self.connection.commit()

    def select_mongo_batches(self, table_name, columns, chunk_size, entity, has_ingest_table=False, languages=['en']):
        try:
            join_statement = ''
            if has_ingest_table:
                join_statement = f"JOIN {entity}_ingest as pi ON pi.{entity}_ingest_id = {table_name}.{table_name}_id"
            query = f"SELECT {columns} FROM {table_name} {join_statement}  WHERE {table_name}.language IN ({str(languages)[1:-1]});"

            def _batches():
                with \
                        self.connection.cursor(name='process-cursor',
                                               cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.itersize = chunk_size
                    cur.arraysize = chunk_size
                    cur.execute(query)
                    while True:
                        batch = [dict(row) for row in cur.fetchmany()]
                        if not batch:
                            break
                        yield batch

            batches = iter(_batches())
            first_batch = next(batches)
            return itertools.chain((first_batch,), batches)
        except Exception:
            print(traceback.format_exc())
            pass

    def close_connection(self):
        self.connection.close()
