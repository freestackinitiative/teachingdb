import duckdb
import pandas as pd
from pathlib import Path

def connect_to_db():
    """Reads in CSV data, creates and returns a DuckDB connection for an in-memory database."""
    con = duckdb.connect(":memory:")
    raw_data_loc = Path.cwd().glob("data/*.csv")
    for data in raw_data_loc:
        df = pd.read_csv(data)
        con.sql(f"CREATE TABLE {data.stem} AS SELECT * FROM df")

    return con