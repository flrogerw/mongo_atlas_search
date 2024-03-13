from sql.SqliteDb import Db
from dotenv import load_dotenv

load_dotenv()


class ListenNotesFetcher:
    def __init__(self, file_location):
        self.db = Db(file_location)

    def fetch(self, table_name, start, end):
        try:
            columns = ["*"]
            rows = self.db.select_pagination(table_name, columns, start, end)
            return rows
        except Exception:
            raise

    def get_records_offset(self, table_name, server_count, server_id):
        try:
            record_count = self.db.get_row_count(table_name)
            last_rowid = self.db.get_last_rowid(table_name)
            if record_count != last_rowid:
                self.db.reindex_table(table_name)
            chunk_size = int(record_count / server_count)
            start = (chunk_size * server_id) + 1
            if server_id + 1 == server_count:
                end = record_count
            else:
                end = (chunk_size * (server_id + 1))
            return start, end
        except Exception:
            raise
