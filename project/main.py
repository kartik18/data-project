from utils.logging import get_logger
from utils.database import Database
from utils.yaml_parser import get_tables
from data_processing.processing import ClientIngestion, OrderIngestion
import pandas as pd

logger = get_logger(__name__)

def read_data(file_path:str):
    logger.info("Reading Data file")
    try: 
        df = pd.read_csv(file_path)
        return df
    except FileNotFoundError as e:
        logger.exception(f'{file_path} does not exists!')
        return False

def main():
    df = read_data("project/data/data_one.csv")
    client_ing_obj = ClientIngestion(table_name='stg_client_dim')
    client_ing_obj.ingestion(dataframe=df)
    ord_obj = OrderIngestion(table_name='orders_fact')
    ord_obj.ingestion(dataframe=df)

    df = read_data("project/data/data_two.csv")
    client_ing_obj = ClientIngestion(table_name='stg_client_dim')
    client_ing_obj.ingestion(dataframe=df)
    ord_obj = OrderIngestion(table_name='orders_fact')
    ord_obj.ingestion(dataframe=df)

if __name__ == "__main__":
    main()