import duckdb
import requests
import pandas as pd
from IPython import get_ipython
from typing import Dict, List, Union, Optional

def setup_notebook():
    # Get the IPython instance
    ipython = get_ipython()
    
    # Load the SQL extension
    ipython.run_line_magic('load_ext', 'sql')
    
    # Set SqlMagic configurations
    ipython.run_line_magic('config', 'SqlMagic.autopandas = True')
    ipython.run_line_magic('config', 'SqlMagic.feedback = False')
    ipython.run_line_magic('config', 'SqlMagic.displaycon = False')
    
    # Set pandas display options
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', 99)



def _get_schema() -> Union[Dict[str, Dict[str, Dict[str, str]]], None]:
    try:
        schema = requests.get("https://raw.githubusercontent.com/freestackinitiative/teachingdb_data/main/schema.json")
        if schema.status_code == 200:
            schema = schema.json()
            return schema["databases"]
        else:
            raise ValueError("Unable to get schema from teachingdb_data. Is it still up?"
                            " Please check https://github.com/freestackinitiative/teachingdb_data"
                            " to ensure that it is still there.")
    except requests.JSONDecodeError as exc:
        print(f"Unable to parse schema: {str(exc)}")

    return None


def _load_db(con: duckdb.DuckDBPyConnection, 
               schema: Dict[str, str]) -> duckdb.DuckDBPyConnection:
    """Creates tables in a DuckDB connection using the given schema"""
    for table_name, table_data in schema.items():
        con.sql(f"CREATE TABLE {table_name} AS "
                f"SELECT * FROM read_csv('{table_data}', "
                "auto_type_candidates = [DATE, TIMESTAMP, INTEGER, FLOAT])")
    return con


def connect_teachdb(con: Optional[duckdb.DuckDBPyConnection] = None, 
                    database: Optional[Union[List[str], str]] = "core"
                    ) -> duckdb.DuckDBPyConnection:
    """Single function to generate the TeachDB database"""
    if con is None:
        con = duckdb.connect()
    schema = _get_schema()
    # Handle requesting multiple databases vs single db
    if isinstance(database, list):
        for db in database:
            try:
                _load_db(con, schema[db])
            except KeyError:
                print(f"Database {db} does not exist in teachdb")
    else:
        _load_db(con, schema[database])

    print(f"Connected to the `teachdb` from the Freestack Initiative.")
    setup_notebook()
    return con
