import psycopg2
import psycopg2.extras


class PostgresDb:

    def __init__(self, username, password, database, host):
        self.database = database
        self.username = username
        self.password = password
        self.host = host

    def connect(self):
        try:
            conn_string = "host={0} user={1} dbname={2} password={3}".format(self.host,
                                                                             self.username,
                                                                             self.database, self.password)
            self.connection = psycopg2.connect(conn_string)
            self.cursor = self.connection.cursor()
        except Exception as err:
            print(err)
            raise

    def truncate_table(self, table_name):
        try:
            self.cursor.execute("TRUNCATE TABLE {table} RESTART IDENTITY".format(table=table_name))
            self.connection.commit()
        except Exception as err:
            raise

    def append_ingest_ids(self, table_name, response):
        try:
            print([(d['file_hash'], d['podcast_uuid']) for d in response])
            argument_string = str([(d['file_hash'], d['podcast_uuid']) for d in response]).strip('[]')
            self.cursor.execute(
                "INSERT INTO podcast.ingest (file_hash, podcast_uuid) VALUES" + argument_string + "RETURNING file_hash,ingest_id")
            result_list_of_tuples = (self.cursor.fetchall())
            self.connection.commit()
            ingest_ids = dict(result_list_of_tuples)
            id_field = '{}_id'.format(table_name)
            for r in response:
                r[id_field] = ingest_ids[r['file_hash']]
            return response

        except Exception:
            raise

    def insert_many(self, table_name, list_of_dicts):
        try:
            columns = ', '.join(list_of_dicts[0].keys())
            place_holders = ','.join(['%s'] * len(list_of_dicts[0].keys()))
            insert_tuples = [tuple(l.values()) for l in list_of_dicts]
            query = "INSERT INTO podcast.{table} ({columns})VALUES ({place_holders})".format(table=table_name,
                                                                                             columns=columns,
                                                                                             place_holders=place_holders)
            psycopg2.extras.execute_batch(self.cursor, query, insert_tuples)
            self.connection.commit()
        except Exception:
            raise

    def close_connection(self):
        self.connection.close()
