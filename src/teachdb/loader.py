"""A collection of paths from the `teachingdb_data repo: https://github.com/freestackinitiative/teachingdb_data"""
import yaml
import pkg_resources

def load_paths(database):
    """Returns the desired collection of paths from the specified database defined in the schema"""
    schema = loader()
    return schema["databases"][database]


def loader():
    path = pkg_resources.resource_filename("teachdb", "config/schema.yml")
    with open(path, "r") as f:
        schema = yaml.load(f, Loader=yaml.SafeLoader)
    return schema
