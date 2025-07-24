# `teachdb` - A free, in-memory database for facilitating hands-on, basic SQL instruction in a notebook environment

By: [Freestack Initiative](https://github.com/freestackinitiative)

**Try `teachdb`**: <a target="_blank" href="https://colab.research.google.com/github/freestackinitiative/teachingdb/blob/main/Using_the_teachdb_Database_Freestack_Initiative.ipynb">
  <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
</a>

**Install `teachdb`**:
```
pip install git+https://github.com/freestackinitiative/teachingdb.git
```

## What is `teachdb`?

`teachdb` is an in-memory micro relational database, powered by `duckdb`. It was made with two types of users in mind: instructors who want to teach SQL concepts, and students who want to learn **and practice** the fundamentals. Combined with a Jupyter Notebook, `teachdb` provides a database that can be used to demonstrate fundamental SQL concepts such as select queries, filtering, aggregations, and joins. It can even be used to introduce more advanced topics such as analytical/window functions, common table expressions (CTEs), data definition language (DDL) commands, etc. For students, it provides a safe environment to learn and experiment with a SQL database without the need for setting up their own server or downloading additional software.

## How do I use it?

Here is all that's needed to use `teachdb` in your Jupyter Notebook:

```python
from teachdb import TeachDB

# Initialize the database
db = TeachDB()

# Set up the notebook environment
db.setup_notebook()

# Connect to SQL magic
connection = db.connection
%sql connection
```

And then, to query your data, create a new cell and use the `%%sql` SQL magic function like so:

```sql
%%sql

SELECT *
FROM company
```

## Python API

### `TeachDB(connection=None, database="core", include_schemas=False)`

The main class for creating and managing TeachDB connections. This class automatically loads sample databases and sets up the environment for SQL instruction.

**Parameters:**
- `connection` (Optional[duckdb.DuckDBPyConnection]): An existing DuckDB connection. If None, creates a new in-memory connection.
- `database` (Union[List[str], str]): The database(s) to load. Defaults to "core". Can be a single database name or a list of database names.
- `include_schemas` (bool): Whether to create separate schemas for each database. Defaults to False.

**Basic Usage:**
```python
from teachdb import TeachDB

# Load the default "core" database
db = TeachDB()
db.setup_notebook()
connection = db.connection
%sql connection
```

**Loading Multiple Databases:**
```python
from teachdb import TeachDB

# Load multiple databases
db = TeachDB(database=["sales_cog_opex", "ds_salaries"])
db.setup_notebook()
connection = db.connection
%sql connection
```

**Using with Existing DuckDB Connection:**
```python
import duckdb
from teachdb import TeachDB

# Create your own connection
conn = duckdb.connect(":memory:")
db = TeachDB(connection=conn, database="core")
db.setup_notebook()
%sql conn
```

### `TeachDB.setup_notebook(**kwargs)`

Configures the notebook environment for optimal SQL instruction, including setting up SQL magic and pandas display options.

**Parameters:**
- `sqlmagic_autopandas` (bool): Enable automatic pandas DataFrame output. Defaults to True.
- `sqlmagic_feedback` (bool): Show SQL magic feedback. Defaults to False.
- `sqlmagic_displaycon` (bool): Display connection info. Defaults to False.
- `display_max_rows` (Optional[int]): Maximum rows to display in pandas output.
- `display_max_columns` (Optional[int]): Maximum columns to display in pandas output.
- `display_width` (Optional[int]): Display width for pandas output.
- `display_max_colwidth` (Optional[int]): Maximum column width. Defaults to 99.
- `**pandas_opts`: Additional pandas display options.

**Example:**
```python
from teachdb import TeachDB

db = TeachDB()
db.setup_notebook(
    display_max_rows=50,
    display_max_columns=10,
    sqlmagic_feedback=True
)
connection = db.connection
%sql connection
```

### `TeachDB.execute_query(query: str)`

Execute a SQL query directly and return results as a list of tuples.

**Parameters:**
- `query` (str): The SQL query to execute.

**Returns:**
- List of tuples representing the query results.

**Example:**
```python
from teachdb import TeachDB

db = TeachDB()
results = db.execute_query("SELECT * FROM company LIMIT 5")
print(results)
```

### `TeachDB.get_database_schema(schema: Optional[str] = None)`

Get metadata about tables in the database by querying DuckDB system tables.

**Parameters:**
- `schema` (Optional[str]): Filter results for a specific schema.

**Returns:**
- List of tuples containing table metadata.

**Example:**
```python
from teachdb import TeachDB

db = TeachDB()
schema_info = db.get_database_schema()
for table_info in schema_info:
    print(table_info)
```

### `TeachDB.load_db(schema: Dict[str, bytes], schema_name: Optional[str] = None)`

Load custom data into the database from a dictionary of CSV data.

**Parameters:**
- `schema` (Dict[str, bytes]): Dictionary with table names as keys and CSV data as bytes as values.
- `overwrite` (bool): Whether or not to overwrite an existing schema/database. Default is `False`.
- `schema_name` (Optional[str]): Name for the database schema.

**Example:**
```python
import pandas as pd
from teachdb import TeachDB

# Create sample data
df = pd.DataFrame({"ColA": list(range(10)), "ColB": list(range(10))})
csv_data = df.to_csv(index=False).encode('utf-8')

# Load into TeachDB
db = TeachDB()
db.load_db(schema={"sample_table": csv_data}, schema_name="custom_data")
db.setup_notebook()
connection = db.connection
%sql connection
```

Now you can query your custom data:
```sql
%%sql 

SELECT *
FROM sample_table;
```

### `get_available_schemas()`

Get information about all available databases in the TeachDB schema.

**Returns:**
- Dictionary containing database schemas and their metadata.

**Example:**
```python
from teachdb import get_available_schemas

schemas = get_available_schemas()
print("Available databases:")
for db_name in schemas.keys():
    print(f"- {db_name}")
```

## Available Databases

TeachDB comes with several pre-built databases for different learning scenarios:

- **core**: The default database with basic tables for learning SQL fundamentals
- **sales_cog_opex**: Sales and operational data for business analytics
- **ds_salaries**: Data science salary information for analytical queries

Use `get_available_schemas()` to see all available databases and their contents.

## Error Handling

TeachDB includes custom exception classes for better error handling:

- `TeachDBConnectionError`: Raised when there are connection issues
- `TeachDBSchemaError`: Raised when there are problems with database schema
- `TeachDBQueryExecutionError`: Raised when query execution fails

These exceptions provide detailed error messages to help with debugging and learning.