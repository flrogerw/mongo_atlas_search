import sqlite3
from sqlite3 import Error
import traceback


class Db:
    def __init__(self, db):
        self.database = db

    def get_connection(self):
        con = sqlite3.connect("{}".format(self.database))
        con.row_factory = sqlite3.Row
        return con

    def create_tables(self, truncate_tables):
        try:
            db_connection = self.get_connection()
            cur = db_connection.cursor()
            res = cur.execute("SELECT name FROM sqlite_master WHERE name='error_log'")
            if res.fetchone() is None:
                fd = open('sql/sqlite_schema.sql', 'r')
                sqlFile = fd.read()
                fd.close()
                sqlCommands = sqlFile.split(';')
                for command in sqlCommands:
                    try:
                        cur.execute(command)
                        db_connection.commit()
                    except Exception as msg:
                        print("Command skipped: ", msg)
                        continue
            elif truncate_tables == 'True':
                self.truncate()
        except Error:
            raise
        finally:
            db_connection.close()

    def insert_many(self, table_name, records):
        db_connection = self.get_connection()
        cur = db_connection.cursor()
        for record in records:
            try:
                columns = ', '.join(record.keys())
                placeholders = ':' + ', :'.join(record.keys())
                query = 'INSERT INTO %s (%s) VALUES (%s)' % (table_name, columns, placeholders)
                cur.execute(query, record)
                db_connection.commit()
            except Error as e:
                print(e)
                db_connection.commit()
                self.insert_one('error_log',
                                {"file_name": 'DATABASE ERROR', "error": str(e), "stack_trace": traceback.format_exc()})
                continue
        db_connection.close()

    def insert_one(self, table_name, record):
        try:
            db_connection = self.get_connection()
            cur = db_connection.cursor()
            columns = ', '.join(record.keys())
            placeholders = ':' + ', :'.join(record.keys())
            query = 'INSERT INTO %s (%s) VALUES (%s)' % (table_name, columns, placeholders)
            cur.execute(query, record)
            db_connection.commit()
        except Error as e:
            print(e)
            raise
        finally:
            db_connection.close()

    def select_all(self, table_name):
        try:
            db_connection = self.get_connection()
            cur = db_connection.cursor()
            cur.execute("SELECT * FROM %s" % table_name)
            return [dict(row) for row in cur.fetchall()]
        except Error as e:
            print(e)
            raise
        finally:
            db_connection.close()

    def select_search_fields(self, table_name, fields, lang):
        try:
            db_connection = self.get_connection()
            cur = db_connection.cursor()
            columns = ', '.join(fields)
            cur.execute("SELECT %s FROM %s WHERE language='%s' AND is_indexed=1" % (columns, table_name, lang))
            # return self.cur.fetchall()
            return [dict(row) for row in cur.fetchall()]
        except Error:
            raise
        finally:
            db_connection.close()

    def truncate(self):
        try:
            db_connection = self.get_connection()
            cur = db_connection.cursor()
            tables = ['error_log', 'podcast_active', 'quarantine', 'podcast_purgatory']
            for table in tables:
                try:
                    cur.execute("UPDATE SQLITE_SEQUENCE SET SEQ=0 WHERE NAME='%s';" % table)
                    cur.execute("DELETE FROM '%s';" % table)
                    db_connection.commit()
                except Exception as e:
                    db_connection.commit()
                    self.insert_one('error_log',
                                    {"file_name": 'DATABASE ERROR', "error": str(e),
                                     "stack_trace": traceback.format_exc()})
                    continue
        except Error:
            raise
        finally:
            db_connection.close()

    def select_pagination(self, table_name, cols, start, end):
        try:
            db_connection = self.get_connection()
            cur = db_connection.cursor()
            columns = ', '.join(cols)
            cur.execute("SELECT %s FROM %s WHERE rowid BETWEEN %d and %d" % (columns, table_name, start, end))
            # return self.cur.fetchall()
            return [dict(row) for row in cur.fetchall()]
        except Error:
            print(traceback.format_exc())
            raise
        finally:
            db_connection.close()

    def get_row_count(self, table_name):
        try:
            db_connection = self.get_connection()
            cur = db_connection.cursor()
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cur.fetchone()
            return count[0]
        except Exception:
            raise
        finally:
            db_connection.commit()
            db_connection.close()

    def get_last_rowid(self, table_name):
        try:
            db_connection = self.get_connection()
            cur = db_connection.cursor()
            cur.execute(f"SELECT MAX(rowid) FROM {table_name}")
            count = cur.fetchone()
            return count[0]
        except Exception:
            raise
        finally:
            db_connection.commit()
            db_connection.close()

    def reindex_table(self, table_name):
        temp_table = f"temp_{table_name}"
        db_connection = self.get_connection()
        cur = db_connection.cursor()
        cur.execute(f"DROP TABLE IF EXISTS original_{table_name}")
        cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
        cur.execute(f"CREATE TABLE {temp_table} AS SELECT * FROM {table_name} WHERE 0")
        cur.execute(f"INSERT INTO {temp_table} SELECT * FROM {table_name}")
        cur.execute(f"ALTER TABLE {table_name} RENAME TO original_{table_name}")
        cur.execute(f"ALTER TABLE {temp_table} RENAME TO {table_name}")
        cur.execute(f"DROP TABLE original_{table_name}")
        db_connection.commit()
        db_connection.close()
