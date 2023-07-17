import pytest
import pandas as pd
import duckdb
from teachdb.teachdb import download_db, connect_db, connect_teachdb  # replace with the name of your module

def test_download_db(mocker):
    # Mock the response from pd.read_csv
    mock_read_csv = mocker.patch('pandas.read_csv', return_value=pd.DataFrame())

    # Call the function
    result = download_db()

    # Check the return type
    assert isinstance(result, dict)
    for df in result.values():
        assert isinstance(df, pd.DataFrame)

    # Check if the pd.read_csv is called the expected number of times
    assert mock_read_csv.call_count == 8


def test_connect_db(mocker):
    # Mock the connection and sql execution
    mock_con = mocker.Mock()

    # Mock data
    mock_data = {
        "table1": pd.DataFrame(),
        "table2": pd.DataFrame()
    }

    # Call the function
    connect_db(mock_con, mock_data)

    # Check if the sql execution method is called the expected number of times
    assert mock_con.sql.call_count == len(mock_data)


def test_connect_teachdb(mocker):
    # Mock the download_db response
    mock_download_db = mocker.patch('teachdb.teachdb.download_db', return_value={
        "table1": pd.DataFrame(),
        "table2": pd.DataFrame()
    })

    # Mock the connection
    mock_con = mocker.Mock()

    # Mock the connect_db method
    mock_connect_db = mocker.patch('teachdb.teachdb.connect_db')

    # Call the function
    connect_teachdb(mock_con)

    # Check if download_db and connect_db methods are called once
    assert mock_download_db.called_once()
    assert mock_connect_db.called_once()
