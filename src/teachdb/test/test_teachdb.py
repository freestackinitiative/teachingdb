import pytest
import pandas as pd
from unittest.mock import MagicMock
from teachdb.loader import load_paths
from teachdb.teachdb import download_db, connect_db, connect_teachdb

# Test data
mock_paths = {"table1": "/path/to/table1.csv", "table2": "/path/to/table2.csv"}

# Mocks
mock_df = MagicMock(spec=pd.DataFrame)
mock_con = MagicMock()


def test_download_db(mocker):
    """Test downloading data as dictionary of dataframes"""
    mocker.patch('pandas.read_csv', return_value=mock_df)
    result = download_db(mock_paths)
    assert isinstance(result, dict)
    assert len(result) == len(mock_paths)
    for table, df in result.items():
        assert table in mock_paths
        assert df == mock_df


def test_connect_db(mocker):
    """Test creating tables in a DuckDB connection"""
    mocker.patch.object(mock_con, 'sql')
    result = connect_db(mock_con, {"test_table": mock_df})
    assert result == mock_con
    mock_con.sql.assert_called_with("CREATE TABLE test_table AS SELECT * FROM table_data")


@pytest.mark.parametrize("database", ["core", "ds_salaries"])
def test_connect_teachdb(mocker, database):
    """Test generating the DuckDB database"""
    mocker.patch('teachdb.loader.load_paths', return_value=mock_paths)
    mocker.patch('teachdb.teachdb.download_db', return_value={"ds_salaries": mock_df})
    mocker.patch('teachdb.teachdb.connect_db', return_value=mock_con)
    mocker.patch('builtins.print')
    result = connect_teachdb(mock_con, database=database)
    assert result == mock_con
