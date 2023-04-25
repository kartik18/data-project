from utils.logging import get_logger
from utils.yaml_parser import get_tables
from utils.database import Database
from data_processing.validations import null_check, duplicate_check, check_if_table_exists

import  abc
import pandas as pd

logger = get_logger(__name__)
db_obj = Database(db_name="test.db")
conn = db_obj.get_connection()


class InterfaceProcessing(abc.ABC):
    tables = get_tables()
    
    def __init__(self, table_name):
        self.table_name = table_name
        self.table_details = InterfaceProcessing.tables[self.table_name]
        self.file_columns = []
        self.table_mapped_columns = []
        self.types = []
        self.validations = {}
        self.required_df = None


    def get_details(self):
        for each_column_detail in self.table_details:
            self.file_columns.append(each_column_detail.get('file_column'))
            self.table_mapped_columns.append(each_column_detail.get('table_mapped_column'))
            self.types.append(each_column_detail.get('type'))
            
            validation_checks = each_column_detail.get('validations')
            if validation_checks:
                for checks in validation_checks.split(","):
                    self.validations[checks.lower().strip()] = self.validations.get(checks.lower().strip(), []) + \
                    [each_column_detail.get('table_mapped_column')]

    def data_type_conversion(self):
        for idx, val in enumerate(self.table_mapped_columns):
            if self.types[idx] in {'REAL', 'DOUBLE'}:
                self.required_df[val] = self.required_df[val].astype('float64')
            elif self.types[idx] in {'INTEGER'}:
                self.required_df[val] = self.required_df[val].astype('int64')
            elif self.types[idx] in {'TEXT'}:
                self.required_df[val] = self.required_df[val].astype('str')
                logger.info(f"-----{val}----")
                logger.info(self.required_df[val].dtypes)
                self.required_df[val] = self.required_df[val].str.capitalize()
            elif self.types[idx] in {'DATE'}:
                self.required_df[val] = pd.to_datetime(self.required_df[val], errors='coerce')

    @abc.abstractmethod
    def ingestion(self, dataframe:pd.DataFrame):
        pass
    
    

class OrderIngestion(InterfaceProcessing):
    def ingestion(self, dataframe:pd.DataFrame):
        self.get_details()
        self.required_df = dataframe[self.file_columns]
        self.required_df = self.required_df.rename(columns=dict(zip(self.file_columns, 
                                                                    self.table_mapped_columns)))

        self.data_type_conversion()

        validation_result = null_check(dataframe=self.required_df, cols=self.validations.get('null_check')) and \
                            duplicate_check(dataframe=self.required_df, cols=self.validations.get('duplicate_check'))
        
        logger.info("Validations passed" if validation_result else "Validations failed")
        
        if validation_result:
            if not check_if_table_exists(table='orders_fact', db_obj=db_obj):
                db_obj.create_table(sql_file="project/sql/ddl_orders_fact.sql")

            try:
                self.required_df.to_sql(name='stg_orders_fact', con=conn, if_exists='replace', index=False)
                db_obj.delete(table_name="orders_fact", filter_clause="order_number in ( SELECT order_number from stg_orders_fact)")
                db_obj.insert(sql_file="project/sql/orders_fact_ingestion.sql")
                
            
            except Exception:
                logger.exception('Failed to ingest data in Orders table!')


class ClientIngestion(InterfaceProcessing):
    def ingestion(self, dataframe: pd.DataFrame):
        conn = db_obj.get_connection()
        self.get_details()
        self.required_df = dataframe[self.file_columns]
        self.required_df = self.required_df.rename(columns=dict(zip(self.file_columns, 
                                                                    self.table_mapped_columns)))

        self.data_type_conversion()

        validation_result = null_check(dataframe=self.required_df, cols=self.validations.get('null_check')) and \
                            duplicate_check(dataframe=self.required_df, cols=self.validations.get('duplicate_check'))
        
        logger.info("Validations passed" if validation_result else "Validations failed")

        if validation_result:
            self.required_df['rank'] = self.required_df.groupby(self.table_mapped_columns)['payment_date'].rank(method='first', ascending=False)
            self.required_df = self.required_df[self.required_df['rank'] == 1]
            self.required_df = self.required_df.drop(columns=['rank'])
            self.required_df = self.required_df.rename(columns={'payment_date':'start_date'})

            if not check_if_table_exists(table='client_dim', db_obj=db_obj):
                db_obj.create_table(sql_file="project/sql/ddl_client_dim.sql")
                self.required_df['end_date'] = pd.Timestamp.max
                self.required_df['end_date'] = pd.to_datetime(self.required_df['end_date'])
                self.required_df['is_current'] = 1
                self.required_df.to_sql(name='stg_client_dim', con=conn, if_exists='replace', index=False)
                db_obj.insert(sql_file="project/sql/client_dim_ingestion.sql")
            
            else:
                try:
                    self.required_df.to_sql(name='stg_client_dim', con=conn, if_exists='replace', index=False)
                    db_obj.insert(sql_file="project/sql/client_dim_scd_ingestion.sql")

                except Exception:
                    logger.exception('Failed to ingest data in Client table!')

