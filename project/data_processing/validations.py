from utils.logging import get_logger
from utils.database import Database
import pandas as pd

logger = get_logger(__name__)

def null_check(dataframe:pd.DataFrame, cols:list[str]):
    output = []
    if cols:
        for col in cols:
            result = dataframe[col].isnull().values.any()
            if result:
                logger.info(f"Column: {col} failed the null check.")
            output.append(result)
    return False if any(output) else True

def duplicate_check(dataframe:pd.DataFrame, cols:list[str]):
    output = []
    if cols:
        for col in cols:
            result = dataframe[col].duplicated().any()
            if result:
                logger.info(f"Column: {col} failed the duplicate check.")
            output.append(result)
    return False if any(output) else True

def check_if_table_exists(table:str, db_obj:Database):
    result = db_obj.select(table_name="sqlite_master", columns=["name"], where_clause=f"type='table' AND name='{table}'")
    return result