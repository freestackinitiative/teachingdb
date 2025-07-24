import duckdb
import requests
import pandas as pd
from IPython import get_ipython
from typing import Dict, List, Union, Optional, Tuple, Any

TeachDBSchema = Union[Dict[str, Dict[str, Dict[str, str]]], None]
TeachDBResult = Union[List[Tuple[Any]], None]
Connection = Union[duckdb.DuckDBPyConnection, None]

class TeachDBConnectionError(Exception):
    pass

class TeachDBSchemaError(Exception):
    pass

class TeachDBQueryExecutionError(Exception):
    pass

def get_available_schemas() -> TeachDBSchema:
    try:
        schema = requests.get("https://raw.githubusercontent.com/freestackinitiative/teachingdb_data/main/schema.json")
        schema.raise_for_status()
        schema = schema.json()
        return schema["databases"]
    except requests.JSONDecodeError as exc:
        raise TeachDBSchemaError(f"Unable to parse schema:\n{schema}\nCaused by exception: {str(exc)}")
    except Exception:
        raise TeachDBSchemaError(
            "Unable to get schema from teachingdb_data. Is it still up?"
            " Please check https://github.com/freestackinitiative/teachingdb_data"
            " to ensure that it is still there."
        )


class TeachDB:
    def __init__(self, 
                 connection: Optional[Connection] = None, 
                 database: Union[List[str], str] = "core", 
                 include_schemas: bool = False):
        
        self._connection: Connection = None
        self._initialize_db_connection(connection=connection)
        self.database: Union[List[str], str] = database
        self.database_schemas: TeachDBSchema = get_available_schemas()
        self.include_schemas: bool = include_schemas
        self.current_schemas: List[str] = []
        self._initialize_db()

    @property
    def connection(self):
        if not self._connection:
            raise TeachDBConnectionError("No connection found")
        
        return self._connection

    def _initialize_db_connection(self, connection: Optional[Connection] = None) -> None:
        if connection is None:
            self._connection = duckdb.connect()
        else:
            self._connection = connection

        return None
    
    def load_db(self, 
                schema: Dict[str, bytes],
                schema_name: Optional[str] = None) -> None:
        """Creates tables in a DuckDB connection using the given schema.
        
        Expects an input of a dictionary with table names as the key and data as the values.
        The data is expected to be a CSV file as bytes.

        Example:
        schema = {
            "restaurants": <csv bytes>
        }
        Args:
            schema (Dictionary[str, bytes]) = The data you want to load into the DuckDB connection. Expects table names as keys 
                                            and CSV data representing the table as values. 
            schema_name (Optional string) = The name you wish to use for the database schema
        """
        if schema_name not in self.current_schemas:
            # Create the schema
            self._connection.sql(f"CREATE SCHEMA {schema_name};")
            # Load the tables
            for table_name, table_data in schema.items():
                # Handle if we want to separate the data into schemas or not
                table_name = table_name 
                if self.include_schemas:
                    table_name = f"{schema_name}.{table_name}"
                
                sql_statement = (
                    f"CREATE TABLE {table_name} AS "
                    f"SELECT * FROM read_csv('{table_data}', "
                    "auto_type_candidates = [DATE, TIMESTAMP, INTEGER, FLOAT])"
                )
                try:
                    self._connection.sql(sql_statement)
                    self.current_schemas.append(schema_name)
                except Exception as ex:
                    raise TeachDBQueryExecutionError(
                        f"Your query, {sql_statement}, failed due to "
                        f"the following exception: {str(ex)}"
                    )

        return None

    def _initialize_db(self) -> None:
        """Single function to generate the TeachDB database"""
        # Handle requesting multiple databases vs single db
        if isinstance(self.database, list):
            for db in self.database: 
                try:   
                    self.load_db(schema_name=db, schema=self.database_schemas[db])
                    self.current_schemas.append(db)
                except KeyError:
                    print(f"Database `{db}` does not exist in teachdb")
                finally:
                    continue
        else:
            self.load_db(schema_name=self.database, schema=self.database_schemas[self.database])

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
        query = "SELECT * FROM duckdb_tables"
        
        if schema:
            query += f" WHERE schema_name='{schema}';"
        
        try:
            database_schema = self._connection.execute(query).fetchall()
            return database_schema
        except Exception as ex:
            raise TeachDBQueryExecutionError(
                f"Your query, {query}, failed due to "
                f"the following exception: {str(ex)}"
            )
        
    def execute_query(self, query: str) -> TeachDBResult:
        try:
            result = self._connection.execute(query).fetchall()
            return result
        except Exception as ex:
            raise TeachDBQueryExecutionError(
                f"Your query, {query}, failed due to "
                f"the following exception: {str(ex)}"
            )

    def setup_notebook(self,
                       sqlmagic_autopandas: bool = True,
                       sqlmagic_feedback: bool = False,
                       sqlmagic_displaycon: bool = False,
                       display_max_rows: Optional[int] = None,
                       display_max_columns: Optional[int] = None,
                       display_width: Optional[int] = None,
                       display_max_colwidth: Optional[int] = 99,
                       **pandas_opts) -> None:
        # Get the IPython instance
        if (ipython := get_ipython()) is None:
            print("No notebook detected")
            return None
        # Load the SQL extension
        ipython.run_line_magic('load_ext', 'sql')
        
        # Set SqlMagic configurations
        ipython.run_line_magic('config', f'SqlMagic.autopandas = {sqlmagic_autopandas}')
        ipython.run_line_magic('config', f'SqlMagic.feedback = {sqlmagic_feedback}')
        ipython.run_line_magic('config', f'SqlMagic.displaycon = {sqlmagic_displaycon}')
        
        # Set pandas display options
        pd.set_option('display.max_rows', display_max_rows)
        pd.set_option('display.max_columns', display_max_columns)
        pd.set_option('display.width', display_width)
        pd.set_option('display.max_colwidth', display_max_colwidth)

        for param, val in pandas_opts.items():
            pd.set_option(param, val)
