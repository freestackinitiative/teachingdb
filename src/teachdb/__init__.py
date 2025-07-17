import duckdb
import requests
import pandas as pd
from IPython import get_ipython
from typing import Dict, List, Union, Optional, Tuple, Any

TeachDBSchema = Union[Dict[str, Dict[str, Dict[str, str]]], None]
Connection = Union[duckdb.DuckDBPyConnection, None]

def get_available_schemas() -> TeachDBSchema:
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


class TeachDB:
    def __init__(self, 
                 connection: Optional[Connection] = None, 
                 database: Union[List[str], str] = "core", 
                 include_schemas: bool = False):
        
        self.connection: Connection = None
        self._initialize_db_connection(connection=connection)
        self.database: Union[List[str], str] = database
        self.database_schemas: TeachDBSchema = get_available_schemas()
        self.include_schemas: bool = include_schemas
        self._initialize_db()

    def _initialize_db_connection(self, 
                               connection: Optional[Connection] = None
                               ) -> None:
        if connection is None:
            self.connection = duckdb.connect()
        else:
            self.connection = connection

        return None
    
    def _load_db(self,
                schema_name: str, 
                schema: Dict[str, str]) -> None:
        """Creates tables in a DuckDB connection using the given schema"""
        # Create the schema
        self.connection.sql(f"CREATE SCHEMA {schema_name};")
        # Load the tables
        for table_name, table_data in schema.items():
            # Handle if we want to separate the data into schemas or not
            if self.include_schemas:
                sql_statement = (
                    f"CREATE TABLE {schema_name}.{table_name} AS "
                    f"SELECT * FROM read_csv('{table_data}', "
                    "auto_type_candidates = [DATE, TIMESTAMP, INTEGER, FLOAT])"
                )
            else:
                sql_statement = (
                    f"CREATE TABLE {table_name} AS "
                    f"SELECT * FROM read_csv('{table_data}', "
                    "auto_type_candidates = [DATE, TIMESTAMP, INTEGER, FLOAT])"
                )
            
            self.connection.sql(sql_statement)

        return None

    def _initialize_db(self) -> None:
        """Single function to generate the TeachDB database"""
        # Handle requesting multiple databases vs single db
        if isinstance(self.database, list):
            for db in self.database:
                try:
                    self._load_db(schema_name=db, schema=self.database_schemas[db])
                except KeyError:
                    print(f"Database `{db}` does not exist in teachdb")
        else:
            self._load_db(schema_name=self.database, schema=self.database_schemas[self.database])

        print(f"Connected to the `teachdb` from the Freestack Initiative.")
        print("If you are in a notebook environment, run the `setup_notebook` method to configure your notebook environment for use.")
        return None

    def get_database_schema(self, schema: Optional[str] = None) -> List[Tuple[Any]]:
        """ Queries the DuckDB System Tables to get table metadata
        
        Args:
            conn (Connection): A DuckDB connection object
            schema (Optional[str]): The schema to filter results for
        Returns:
            A list of tuples representing rows in the system table
        """
        query = f"SELECT * FROM duckdb_tables"
        
        if schema:
            query += f" WHERE schema_name='{schema}';"
        
        return self.connection.execute(query).fetchall()

    def setup_notebook(self) -> None:
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
