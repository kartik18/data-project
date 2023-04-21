from utils.logging import get_logger
from utils.database import Database
from utils.yaml_parser import get_tables
import pandas as pd
import sqlite3

logger = get_logger(__name__)
db_obj = Database(db_name="test.db")
tables = get_tables()
conn = db_obj.get_connection()

def read_data(file_path):
    logger.info("Reading Data file")
    try: 
        df = pd.read_csv(file_path)
        logger.info("Reading Data file")
        return df
    except FileNotFoundError:
        logger.exception(f'{file_path} does not exists!')
        return False


def check_if_table_exists(table):
    result = db_obj.select(table_name="sqlite_master", columns=["name"], where_clause=f"type='table' AND name='{table}'")
    return result


def client_ingestion(dataframe:pd.DataFrame, table_name):
    table_details = tables[table_name]
    file_columns = []
    table_mapped_columns = [] 
    types = []
    validations = {}
     
    for each_column_detail in table_details:
        file_columns.append(each_column_detail.get('file_column'))
        table_mapped_columns.append(each_column_detail.get('table_mapped_column'))
        types.append(each_column_detail.get('type'))
        
        
        column_validation = each_column_detail.get('validations')
        if column_validation:
            for checks in column_validation.split(","):
                validations[checks.lower().strip()] = validations.get(checks.lower().strip(), []) + \
                [each_column_detail.get('table_mapped_column')]


    required_df = dataframe[file_columns]
    required_df = required_df.rename(columns=dict(zip(file_columns, table_mapped_columns)))

    for idx, val in enumerate(table_mapped_columns):
        if types[idx] in {'REAL', 'DOUBLE'}:
            required_df[val] = required_df[val].astype('float64')
        elif types[idx] in {'INTEGER'}:
            required_df[val] = required_df[val].astype('int64')
        elif types[idx] in {'TEXT'}:
            required_df[val] = required_df[val].astype('str')
            required_df[val] = required_df[val].str.capitalize()
        elif types[idx] in {'DATE'}:
            required_df[val] = pd.to_datetime(required_df[val], errors='coerce').dt.date    # No effect

    validation_result = null_check(dataframe=required_df, cols=validations.get('null_check')) and \
                        duplicate_check(dataframe=required_df, cols=validations.get('duplicate_check'))

    logger.info("Validations passed" if validation_result else "Validations failed")
    
    if validation_result:
        required_df['rank'] = required_df.groupby(table_mapped_columns)['payment_date'].rank(method='first', ascending=False)
        required_df = required_df[required_df['rank'] == 1]
        required_df = required_df.drop(columns=['rank'])
        required_df = required_df.rename(columns={'payment_date':'start_date'})


        if not check_if_table_exists(table='client_dim'):
            db_obj.create_table(sql_file="sql/ddl_client_dim.sql")
            required_df['end_date'] = pd.Timestamp.max
            required_df['end_date'] = pd.to_datetime(required_df['end_date']).dt.date #No effect
            required_df['is_current'] = 1
            required_df.to_sql(name='stg_client_dim', con=conn, if_exists='replace', index=False)
            db_obj.insert(sql_file="sql/client_dim_ingestion.sql")
            
        else:
            try:
                required_df.to_sql(name='stg_client_dim', con=conn, if_exists='replace', index=False)
                db_obj.insert(sql_file="sql/client_dim_scd_ingestion.sql")

            except Exception:
                logger.exception('Failed to ingest data in Orders table!')




def null_check(dataframe:pd.DataFrame, cols):
    output = []
    if cols:
        for col in cols:
            result = dataframe[col].isnull().values.any()
            if result:
                logger.info(f"Column: {col} failed the null check.")
            output.append(result)
    return False if any(output) else True

def duplicate_check(dataframe:pd.DataFrame, cols):
    output = []
    if cols:
        for col in cols:
            result = dataframe[col].duplicated().any()
            if result:
                logger.info(f"Column: {col} failed the duplicate check.")
            output.append(result)
    return False if any(output) else True
  

def order_ingestion(dataframe:pd.DataFrame, table_name): 
    table_details = tables[table_name]
    file_columns = []
    table_mapped_columns = [] 
    types = []
    validations = {}
    
    for each_column_detail in table_details:
        file_columns.append(each_column_detail.get('file_column'))
        table_mapped_columns.append(each_column_detail.get('table_mapped_column'))
        types.append(each_column_detail.get('type'))
        
        for checks in each_column_detail.get('validations').split(","):
            validations[checks.lower().strip()] = validations.get(checks.lower().strip(), []) + \
            [each_column_detail.get('table_mapped_column')]



    required_df = dataframe[file_columns]
    required_df = required_df.rename(columns=dict(zip(file_columns, table_mapped_columns)))

    for idx, val in enumerate(table_mapped_columns):
        if types[idx] in {'REAL', 'DOUBLE'}:
            required_df[val] = required_df[val].astype('float64')
        elif types[idx] in {'INTEGER'}:
            required_df[val] = required_df[val].astype('int64')
        elif types[idx] in {'TEXT'}:
            required_df[val] = required_df[val].astype('str')
            required_df[val] = required_df[val].str.capitalize()
        elif types[idx] in {'DATE'}:
            required_df[val] = pd.to_datetime(required_df[val], errors='coerce')    
        
    # validations
    validation_result = null_check(dataframe=required_df, cols=validations.get('null_check')) and \
                        duplicate_check(dataframe=required_df, cols=validations.get('duplicate_check'))
    
    logger.info("Validations passed" if validation_result else "Validations failed")

    if validation_result:

        if not check_if_table_exists(table='orders_fact'):
            db_obj.create_table(sql_file="sql/ddl_orders_fact.sql")

        try:
            # delete records from table
            required_df.to_sql(name='stg_orders_fact', con=conn, if_exists='replace', index=False)
            db_obj.delete(table_name="orders_fact", filter_clause="order_number in ( SELECT order_number from stg_orders_fact)")
            db_obj.insert(sql_file="sql/orders_fact_ingestion.sql")
            # required_df.to_sql(name='orders_fact', con=conn, if_exists='append', index=False)
        
        except Exception:
            logger.exception('Failed to ingest data in Orders table!')

                


def main():
    df = read_data("data/data_one.csv")
    client_ingestion(dataframe=df, table_name='stg_client_dim')
    df = read_data("data/data_two.csv")
    client_ingestion(dataframe=df, table_name='stg_client_dim')
    #order_ingestion(dataframe=df, table_name='orders_fact')

if __name__ == "__main__":
    main()