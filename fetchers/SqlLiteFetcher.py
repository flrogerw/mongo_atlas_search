from sql.SqliteDb import Db
from dotenv import load_dotenv

load_dotenv()


class SqlLiteFetcher:
    """
    A class to interact with an SQLite database, allowing data fetching
    with pagination and offset calculations for distributed processing.
    """

    def __init__(self, file_location: str) -> None:
        """
        Initialize the SqlLiteFetcher with a database file location.

        Args:
            file_location (str): Path to the SQLite database file.
        """
        try:
            self.db = Db(file_location)
        except Exception as e:
            raise RuntimeError(f"Error initializing database connection: {e}")

    def fetch(self, table_name: str, start: int, end: int) -> list:
        """
        Fetch paginated records from the database table.

        Args:
            table_name (str): Name of the table to fetch data from.
            start (int): Starting index for pagination.
            end (int): Ending index for pagination.

        Returns:
            list: List of fetched records.
        """
        try:
            columns = ["*"]  # Select all columns
            # Fetch paginated results (specific to Listen Notes dataset)
            rows = self.db.select_listenotes_pagination(table_name, columns, start, end)
            return rows
        except Exception as e:
            raise RuntimeError(f"Error fetching data from {table_name}: {e}")

    def get_records_offset(self, table_name: str, server_count: int, server_id: int) -> tuple[int, int]:
        """
        Calculate the offset range for distributed servers processing a table.

        Args:
            table_name (str): Name of the table.
            server_count (int): Total number of servers.
            server_id (int): Current server ID.

        Returns:
            tuple[int, int]: A tuple containing (start, end) row indices for this server.
        """
        try:
            record_count = self.db.get_row_count(table_name)
            last_rowid = self.db.get_last_rowid(table_name)

            # Ensure table is indexed properly
            if record_count != last_rowid:
                self.db.reindex_table(table_name)

            # Compute chunk size and assign offset range
            chunk_size = record_count // server_count
            start = (chunk_size * server_id) + 1

            # Assign the end index for the last server
            end = record_count if (server_id + 1) == server_count else (chunk_size * (server_id + 1))

            return start, end
        except ZeroDivisionError:
            raise ValueError("Server count must be greater than zero.")
        except Exception as e:
            raise RuntimeError(f"Error calculating record offset for {table_name}: {e}")