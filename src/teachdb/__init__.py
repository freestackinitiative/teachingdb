import duckdb
import requests
import pandas as pd
from IPython import get_ipython
from typing import Dict, List, Union, Optional, Tuple, Any

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


def get_available_schemas() -> Union[Dict[str, Dict[str, Dict[str, str]]], None]:
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


def _load_db(conn: duckdb.DuckDBPyConnection,
             schema_name: str, 
             schema: Dict[str, str]) -> duckdb.DuckDBPyConnection:
    """Creates tables in a DuckDB connection using the given schema"""
    # Create the schema
    conn.sql(f"CREATE SCHEMA {schema_name};")
    # Load the tables
    for table_name, table_data in schema.items():
        conn.sql(f"CREATE TABLE {schema_name}.{table_name} AS "
                f"SELECT * FROM read_csv('{table_data}', "
                "auto_type_candidates = [DATE, TIMESTAMP, INTEGER, FLOAT])")
    return conn


def connect_teachdb(conn: Optional[duckdb.DuckDBPyConnection] = None, 
                    database: Optional[Union[List[str], str]] = "core"
                    ) -> duckdb.DuckDBPyConnection:
    """Single function to generate the TeachDB database"""
    if conn is None:
        conn = duckdb.connect()

    schema = get_available_schemas()
    # Handle requesting multiple databases vs single db
    if isinstance(database, list):
        for db in database:
            try:
                _load_db(conn=conn, schema_name=db, schema=schema[db])
            except KeyError:
                print(f"Database `{db}` does not exist in teachdb")
    else:
        _load_db(conn=conn, schema_name=database, schema=schema[database])

    print(f"Connected to the `teachdb` from the Freestack Initiative.")
    print("If you are in a notebook environment, import and run `setup_notebook` to configure your notebook environment for use.")
    return conn


def get_database_schema(conn: duckdb.DuckDBPyConnection, schema: Optional[str] = None) -> List[Tuple[Any]]:
    """ Queries the DuckDB System Tables to get table metadata
    
    Args:
        conn (duckdb.DuckDBPyConnection): A DuckDB connection object
        schema (Optional[str]): The schema to filter results for
    Returns:
        A list of tuples representing rows in the system table
    """
    query = f"SELECT * FROM duckdb_tables"
    
    if schema:
        query += f" WHERE schema_name='{schema}';"
    
    return conn.execute(query).fetchall()
