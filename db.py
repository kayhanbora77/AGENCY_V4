import os
from contextlib import contextmanager
from pathlib import Path
import duckdb
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DEFAULT_DB_PATH = os.getenv("DUCKDB_PATH", r"C:\DuckDB\my_db.duckdb")


def get_connection(db_path: str | Path = DEFAULT_DB_PATH, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Open and return a DuckDB connection."""
    path_str = str(db_path)
    return duckdb.connect(database=path_str, read_only=read_only)


@contextmanager
def get_db(db_path: str | Path = DEFAULT_DB_PATH, read_only: bool = False):
    """Context manager for managing a DuckDB database connection."""
    conn = get_connection(db_path=db_path, read_only=read_only)
    try:
        yield conn
    finally:
        conn.close()


def list_tables(db_path: str | Path = DEFAULT_DB_PATH) -> list[str]:
    """Retrieve all table names in the DuckDB database."""
    with get_db(db_path=db_path, read_only=True) as conn:
        tables = conn.execute("SHOW TABLES").fetchall()
        return [table[0] for table in tables]
