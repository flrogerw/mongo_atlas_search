from sql.SqliteDb import Db
from dotenv import load_dotenv

load_dotenv()


class ListenNotesFetcher:
    def __init__(self, file_location):
        self.db = Db(file_location)

    def fetch(self, table_name, limit, offset=0):
        try:
            columns = ["*"]
            rows = self.db.select_pagination(table_name, columns, limit, offset)
            return rows
        except Exception:
            raise

    def get_row_count(self, table_name):
        try:
            row_count = self.db.get_row_count(table_name)
            return row_count
        except Exception:
            raise

    def get_records_offset(self, table_name, server_count, server_id):
        try:
            record_count = self.get_row_count(table_name)
            chunk_size = int(record_count / server_count)
            offset = (chunk_size * server_id) + 1
            if server_count == 1:
                limit = record_count
            elif server_id + 1 == server_count:
                limit = record_count - offset
            else:
                limit = chunk_size
            return offset, limit
        except Exception:
            raise


