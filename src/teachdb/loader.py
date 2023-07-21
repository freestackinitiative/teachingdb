"""A collection of paths from the `teachingdb_data repo: https://github.com/freestackinitiative/teachingdb_data"""
import yaml
import pkg_resources

def _load_paths(database):
    """Returns the desired collection of paths from the specified database defined in the schema"""
    schema = get_schema()
    return schema["databases"][database]


def get_schema():
    path = pkg_resources.resource_filename("teachdb", "config/schema.yml")
    with open(path, "r") as f:
        schema = yaml.load(f, Loader=yaml.SafeLoader)
    return schema
