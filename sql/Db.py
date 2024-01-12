import sqlite3
import traceback


class Db:
    def __init__(self, db='project.db'):
        self.con = sqlite3.connect("sql/{}".format(db))
        self.con.row_factory = sqlite3.Row
        self.cur = self.con.cursor()

    def create_tables(self, truncate_tables):
        try:
            res = self.cur.execute("SELECT name FROM sqlite_master WHERE name='error_log'")
            if res.fetchone() is None:
                fd = open('sql/sqlite_schema.sql', 'r')
                sqlFile = fd.read()
                fd.close()
                sqlCommands = sqlFile.split(';')
                for command in sqlCommands:
                    try:
                        self.cur.execute(command)
                        self.con.commit()
                    except Exception as msg:
                        print("Command skipped: ", msg)
            elif truncate_tables == 'True':
                self.truncate()
        except self.con.Error:
            raise

    def insert_many(self, table_name, records):
        for record in records:
            try:
                columns = ', '.join(record.keys())
                placeholders = ':' + ', :'.join(record.keys())
                query = 'INSERT INTO %s (%s) VALUES (%s)' % (table_name, columns, placeholders)
                self.cur.execute(query, record)
                self.con.commit()
            except self.con.Error as e:
                self.insert_one('error_log', {"file_name": 'DATABASE ERROR', "error": str(e), "stack_trace": traceback.format_exc()})
                pass

    def insert_one(self, table_name, record):
        try:
            columns = ', '.join(record.keys())
            placeholders = ':' + ', :'.join(record.keys())
            query = 'INSERT INTO %s (%s) VALUES (%s)' % (table_name, columns, placeholders)
            self.cur.execute(query, record)
            self.con.commit()
        except self.con.Error as e:
            self.insert_one('error_log', {"file_name": 'DATABASE ERROR', "error": str(e), "stack_trace": traceback.format_exc()})
            pass

    def select_all(self, table_name):
        try:
            self.cur.execute("SELECT * FROM %s" % (table_name))
            # return self.cur.fetchall()
            return [dict(row) for row in self.cur.fetchall()]
        except self.con.Error:
            raise

    def select_search_fields(self, table_name, fields, lang):
        try:
            columns = ', '.join(fields)
            self.cur.execute("SELECT %s FROM %s WHERE language='%s' AND is_indexed=1" % (columns, table_name, lang))
            # return self.cur.fetchall()
            return [dict(row) for row in self.cur.fetchall()]
        except self.con.Error:
            raise

    def truncate(self):
        try:
            tables = ['error_log', 'podcast_active', 'quarantine', 'podcast_purgatory']
            for table in tables:
                self.cur.execute("UPDATE SQLITE_SEQUENCE SET SEQ=0 WHERE NAME='%s';" % table)
                self.cur.execute("DELETE FROM '%s';" % table)
            self.con.commit()
        except self.con.Error:
            raise

    def select_pagination(self, table_name, cols, limit, offset):
        try:
            columns = ', '.join(cols)
            self.cur.execute("SELECT %s FROM %s LIMIT %d OFFSET %d" % (columns, table_name, limit, offset))
            # return self.cur.fetchall()
            return [dict(row) for row in self.cur.fetchall()]
        except self.con.Error:
            raise
