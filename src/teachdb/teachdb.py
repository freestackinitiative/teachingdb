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


def _extract_data(database="core"):
    raw_data = []
    if isinstance(database, list):
        for db in database:
            paths = _load_paths(db)
            data = _download_db(paths)
            raw_data.append(data)
    else:
        raw_data.append(_download_db(_load_paths(database)))
    return raw_data


def connect_teachdb(con=None, database="core"):
    """Single function to generate the DuckDB database"""
    if con is None:
        con = duckdb.connect()
    raw_data = _extract_data(database)
    connection = None
    for data in raw_data:
        connection = connect_db(con, data)
    print(f"Connected to the `teachdb` from the Freestack Initiative. You are using the `{database}` database(s).")
    return connection
