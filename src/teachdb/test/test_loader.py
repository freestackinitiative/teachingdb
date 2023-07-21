import pytest
from unittest.mock import mock_open, MagicMock
from teachdb.loader import _load_paths, get_schema

# Mocks
mock_yaml = MagicMock()

def test_loader(mocker):
    """Test the loader function"""
    mock_open_function = mock_open(read_data="databases: {core: {table1: path1, table2: path2}}")
    mocker.patch('builtins.open', mock_open_function)
    mocker.patch('yaml.load', return_value=mock_yaml)
    mocker.patch('pkg_resources.resource_filename', return_value='teachdb/config/schema.yml')
    
    schema = get_schema()
    
    mock_open_function.assert_called_once_with('teachdb/config/schema.yml', 'r')
    assert schema == mock_yaml

@pytest.mark.parametrize("database", ["core", "test_db"])
def test_load_paths(mocker, database):
    """Test the load_paths function"""
    mock_loader = mocker.patch('teachdb.loader.get_schema', return_value={"databases": {database: {}}})
    
    paths = _load_paths(database)
    
    mock_loader.assert_called_once()
    assert paths == {}

