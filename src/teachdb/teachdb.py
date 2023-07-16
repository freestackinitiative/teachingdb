import duckdb
import pandas as pd


def download_db():
    """Returns a dictionary of dataframes as raw data"""
    raw_data_loc = [
        {"table": "salesman", "path": "https://raw.githubusercontent.com/freestackinitiative/coop_sql_notebooks/main/data/SALESMAN.csv"},
        {"table": "customer", "path":"https://raw.githubusercontent.com/freestackinitiative/coop_sql_notebooks/main/data/CUSTOMER.csv"},
        {"table": "order_details", "path":"https://raw.githubusercontent.com/freestackinitiative/coop_sql_notebooks/main/data/ORDERDETAILS.csv"},
        {"table": "foods", "path": "https://raw.githubusercontent.com/freestackinitiative/coop_sql_notebooks/main/data/foods.csv"},
        {"table": "company", "path": "https://raw.githubusercontent.com/freestackinitiative/coop_sql_notebooks/main/data/company.csv"},
        {"table": "movies", "path": "https://raw.githubusercontent.com/freestackinitiative/coop_sql_notebooks/main/data/movies.csv"},
        {"table": "boxoffice", "path": "https://raw.githubusercontent.com/freestackinitiative/coop_sql_notebooks/main/data/boxoffice.csv"},
        {"table": "employees", "path": "https://raw.githubusercontent.com/freestackinitiative/coop_sql_notebooks/main/data/employees.csv"}
    ]
    raw_data = {}
    for data in raw_data_loc:
        raw_data[data['table']] = pd.read_csv(data['path'])
    
    return raw_data