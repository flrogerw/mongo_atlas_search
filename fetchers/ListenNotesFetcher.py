from sql.SqliteDb import Db
from dotenv import load_dotenv
load_dotenv()


class ListenNotesFetcher:
    def __init__(self, file_location):
        self.db = Db(file_location)

    def fetch(self, table_name, limit):
        try:
            offset = 0
            columns = ["title", "description", "language", "rss"]
            rows = self.db.select_pagination(table_name, columns, limit, offset)
            return rows

        except Exception:
            raise
