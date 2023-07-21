"""A collection of paths from the `teachingdb_data repo: https://github.com/freestackinitiative/teachingdb_data"""
import yaml
import pkg_resources

CORE = [
    {"table": "salesman", "path": "https://raw.githubusercontent.com/freestackinitiative/teachingdb_data/main/data/core/SALESMAN.csv"},
    {"table": "customer", "path":"https://raw.githubusercontent.com/freestackinitiative/teachingdb_data/main/data/core/CUSTOMER.csv"},
    {"table": "order_details", "path":"https://raw.githubusercontent.com/freestackinitiative/teachingdb_data/main/data/core/ORDERDETAILS.csv"},
    {"table": "foods", "path": "https://raw.githubusercontent.com/freestackinitiative/teachingdb_data/main/data/core/foods.csv"},
    {"table": "company", "path": "https://raw.githubusercontent.com/freestackinitiative/teachingdb_data/main/data/core/company.csv"},
    {"table": "movies", "path": "https://raw.githubusercontent.com/freestackinitiative/teachingdb_data/main/data/core/movies.csv"},
    {"table": "boxoffice", "path": "https://raw.githubusercontent.com/freestackinitiative/teachingdb_data/main/data/core/boxoffice.csv"},
    {"table": "employees", "path": "https://raw.githubusercontent.com/freestackinitiative/teachingdb_data/main/data/core/employees.csv"}
]

DS_SALARIES = [
    {"table": "countries", "path": "https://raw.githubusercontent.com/freestackinitiative/teachingdb_data/main/data/ds_salaries/countries.csv"},
    {"table": "ds_salaries", "path":"https://raw.githubusercontent.com/freestackinitiative/teachingdb_data/main/data/ds_salaries/ds_salaries.csv"},
    {"table": "employment_types", "path":"https://raw.githubusercontent.com/freestackinitiative/teachingdb_data/main/data/ds_salaries/employment_types.csv"},
    {"table": "experience_levels", "path": "https://raw.githubusercontent.com/freestackinitiative/teachingdb_data/main/data/ds_salaries/experience_levels.csv"},
    {"table": "usd_exchange_rates", "path": "https://raw.githubusercontent.com/freestackinitiative/teachingdb_data/main/data/ds_salaries/usd_exchange_rates.csv"}
]

def load_paths(database):
    """Returns the desired collection of paths from the specified database"""
    data = {
        "core": CORE,
        "ds_salaries": DS_SALARIES
    }
    return data[database]


def loader():
    path = pkg_resources.resource_filename("teachdb", "config/schema.yml")
    with open(path, "r") as f:
        schema = yaml.load(f, Loader=yaml.SafeLoader)
    print(schema)