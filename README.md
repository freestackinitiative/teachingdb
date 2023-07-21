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

`teachdb` is an in-memory micro relational database, powered by `duckdb`. It was made with two types of users in mind: instructors who want to teach SQL concepts, and students who want to learn **and practice**  the fundamentals. Combined with a Jupyter Notebook, `teachdb` provides a database that can be used to demonstrate fundamental SQL concepts such as select queries, filtering, aggregations, and joins. It can even be used to introduce more advanced topics such as analytical/window functions, common table expressions (CTEs), data definition language (DDL) commands, etc. For students, it provides a safe environment to learn and experiment with a SQL database without the need for setting up your their own server or downloading additional software.

## How do I use it?

Here is all that's needed to use `teachdb` in your Jupyter Notebook:
```python
import duckdb
from teachdb.teachdb import connect_teachdb

%load_ext sql
con = duckdb.connect(":memory:")
connect_teachdb(con)
%sql con
```

And then, to query your data, create a new cell and use the `%%sql` SQL magic function like so:
```SQL
%%sql

SELECT *
FROM company
```

## Python API

### `teachdb.teachdb.connect_db(connection, data)`

This method creates the tables in the `DuckDB` connection object using the supplied `data`. 

Expects a `DuckDB` connection object and a dictionary of `pandas` data frames in the following format: `{"table_name": pd.DataFrame}`

```python
import pandas as pd
import duckdb
from teachdb.teachdb import connect_db

df = pd.DataFrame({"ColA": list(range(10)), "ColB": list(range(10))})
data = {"sample_table": df}
con = duckdb.connect(":memory:")
connect_db(con, data)
%load_ext sql
%sql con
```

Now you can query your dataframe:
```SQL
%%sql 

SELECT *
FROM sample_table;
```

### `teachdb.teachdb.connect_teachdb(connection, database="core")`

This method is used to load your desired database into your environment using a `duckdb` connection. It takes a `duckdb` connection and an optional `database` specification and loads in the requested data. The database that you specify here must exist in the `teachdb` schema. Use the `get_schema` method to see all the available databases through `teachdb`.    

```python
import duckdb
from teachdb.teachdb import connect_teachdb

%load_ext sql
con = duckdb.connect(":memory:")
connect_teachdb(con)
%sql con
```

### `teachdb.loader.get_schema()`

The `get_schema` method returns a dictionary of databases, tables, and their respective paths that are available in `teachdb`.

```python
from teachdb.loader import get_schema

schema = get_schema()
print(schema)
```