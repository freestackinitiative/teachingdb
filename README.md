# `teachdb` - A free, in-memory database for facilitating hands-on, basic SQL instruction in a notebook environment

By: [Freestack Initiative](https://github.com/freestackinitiative)

**Try `teachdb`**: 

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

### `teachdb.teachdb.download_db()`

Retrieves the data tables that make up `teachdb` and returns a dictionary of `pandas` Data Frames with table names as the keys. It accepts no arguments.

```python
from teachdb.teachdb import download_db

database = download_db()
``` 

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
```

Now you can query your dataframe:
```SQL
%%sql 

SELECT *
FROM sample_table;
```

### `teachdb.teachdb.connect_teachdb(connection)`

This method is a convenience wrapper around the `download_db` and `connect_db` methods. It is a convenience function used primarily to simplify usage. All it expects is a `DuckDB` connection as input. It downloads the data and connects it to the connection object it received, thus making the data query-able.

```python
import duckdb
from teachdb.teachdb import connect_teachdb

%load_ext sql
con = duckdb.connect(":memory:")
connect_teachdb(con)
%sql con
```