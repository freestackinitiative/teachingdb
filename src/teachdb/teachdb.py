import duckdb
import pandas as pd
from teachdb.loader import load_paths, loader

def download_db(paths):
    """Returns a dictionary of dataframes as raw data"""
    raw_data = {data["table"]: pd.read_csv(data["path"]) for data in paths}
    return raw_data


def connect_db(con, db):
    """Creates tables in a DuckDB connection using the given dictionary of dataframes"""
    for table_name, df in db.items():
        table_data = df
        con.sql(f"CREATE TABLE {table_name} AS SELECT * FROM table_data")


def connect_teachdb(con, database="core"):
    """Single function to generate the DuckDB database"""
    paths = load_paths(database)
    raw_data = download_db(paths=paths)
    connect_db(con, raw_data)
    print(f"Connected to the `teachdb` from the Freestack Initiative. You are using the `{database}` database.")


def tester():
    loader()
