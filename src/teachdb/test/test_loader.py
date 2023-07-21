import pytest
from teachdb.loader import load_paths, CORE, DS_SALARIES

def test_load_paths():
    """Test load_paths function"""
    # Check the returned value for the "core" database
    result_core = load_paths(database="core")
    assert result_core == CORE

    # Check the returned value for the "ds_salaries" database
    result_ds_salaries = load_paths(database="ds_salaries")
    assert result_ds_salaries == DS_SALARIES

    # Check if the function raises a KeyError for an unknown database
    with pytest.raises(KeyError):
        load_paths(database="unknown")

