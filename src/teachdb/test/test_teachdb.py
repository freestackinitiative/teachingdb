import duckdb
import pandas as pd
import pytest
from unittest import mock
from teachdb.loader import load_paths
from teachdb.teachdb import download_db, connect_db, connect_teachdb

# Mock data to simulate what load_paths might return
paths_mock = [
    {"table": "table1", "path": "path1.csv"},
    {"table": "table2", "path": "path2.csv"},
]

# Mock data to simulate what pandas.read_csv might return
csv_data_mock = pd.DataFrame({'Column1': [1, 2], 'Column2': ['a', 'b']})

@mock.patch('pandas.read_csv', return_value=csv_data_mock)
def test_download_db(mock_read_csv):
    """Test download_db function"""
    result = download_db(paths=paths_mock)

    # Check if the returned object is a dictionary
    assert isinstance(result, dict)
    # Check if the keys in the dictionary match the table names in paths_mock
    assert set(result.keys()) == set([data["table"] for data in paths_mock])
    # Check if the dataframes match the mock data
    for df in result.values():
        pd.testing.assert_frame_equal(df, csv_data_mock)


@mock.patch('duckdb.connect')
def test_connect_db(mock_con):
    """Test connect_db function"""
    raw_data = {data["table"]: csv_data_mock for data in paths_mock}
    connect_db(mock_con, raw_data)
    # Check if the function called con.sql the correct number of times
    assert mock_con.sql.call_count == len(paths_mock)

