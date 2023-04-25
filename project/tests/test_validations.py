import os, sys, sqlite3
import pandas as  pd 
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from data_processing.validations import null_check, duplicate_check
from utils.database import Database


def test_false_null_check():
    df = pd.DataFrame({'A': ['ABC', 'GHE', 'DEF'], 'B': [1, 2, 3]})
    assert null_check(dataframe=df, cols=['A']) == True

def test_true_null_check():
    df = pd.DataFrame({'A': ['ABC', None, 'DEF'], 'B': [1, 2, 3]})
    assert null_check(dataframe=df, cols=['A']) == False

def test_false_duplicate_check():
    df = pd.DataFrame({'A': ['ABC', 'PQT', 'DEF'], 'B': [1, 2, 3]})
    assert duplicate_check(dataframe=df, cols=['A']) == True

def test_true_duplicate_check():
    df = pd.DataFrame({'A': ['ABC', 'ABC', 'DEF'], 'B': [1, 2, 3]})
    assert duplicate_check(dataframe=df, cols=['A']) == False
