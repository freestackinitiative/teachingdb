import duckdb
import pandas as pd
from teachdb.loader import _load_paths

def _download_db(paths):
    """Returns a dictionary of dataframes as raw data"""
    raw_data = {table: pd.read_csv(path) for table, path in paths.items()}
    return raw_data


def connect_db(con, db):
    """Creates tables in a DuckDB connection using the given dictionary of dataframes"""
    for table_name, df in db.items():
        table_data = df
        con.sql(f"CREATE TABLE {table_name} AS SELECT * FROM table_data")
    return con


def _multi_connect_db(con, databases):
    
    for db in databases:
        paths = _load_paths(db)
        data = _download_db(paths)
        connect_db(con, data)

    return con


def connect_teachdb(con=None, database="core", databases=None):
    """Single function to generate the DuckDB database"""
    if con is None:
        con = duckdb.connect()
    # Handle requesting multiple databases vs single db
    if databases:
        _multi_connect_db(con, databases)
    else:
        raw_data = _download_db(_load_paths(database))
        connect_db(con, raw_data)
    print(f"Connected to the `teachdb` from the Freestack Initiative.")
    return con
