import duckdb
import pandas as pd
from pathlib import Path

def download_db():
    """Returns a dictionary of dataframes as raw data"""
    raw_data_loc = Path.cwd().glob("data/*.csv")
    raw_data = {}
    for data in raw_data_loc:
        raw_data[data.stem] = pd.read_csv(data)
    
    return raw_data