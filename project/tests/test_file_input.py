import os, sys
import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from main import read_data

def test_read_data():
    file_path = "project/data/data_one.csv"
    assert isinstance(read_data(file_path), pd.DataFrame)