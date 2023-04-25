from utils.logging import get_logger
from typing import Dict
import sqlite3

logger = get_logger(__name__)

class Database:
    _obj = None

    # Singleton instance for Database 
    def __new__(cls, db_name:str):
        if cls._obj is None:
            cls._obj = super(Database, cls).__new__(cls)
            cls._obj.db_name = db_name
            cls._obj.connection = None
        return cls._obj

    # Get connection
    def get_connection(self):
        if self.connection is None:
            self.connection = sqlite3.connect(self.db_name)
        return self.connection

    # Close connection
    def close_connection(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    # Create table
    def create_table(self, table_name:str=None, columns:Dict[str, str]=None, sql_file:str=None):
        conn = self.get_connection()
        cursor = conn.cursor()

        if sql_file:
            try:
                with open(sql_file, 'r') as f:
                    create_query = f.read()
                    logger.info(create_query)
                    cursor.executescript(create_query)
            except Exception as e:
                raise e
        else:
            columns_str = ', '.join([f'{column} {columns[column]}' for column in columns])
            create_query = f'CREATE TABLE IF NOT EXISTS {table_name} ({columns_str})'
            logger.info(create_query)
            try:
                cursor.execute(create_query)
            except Exception as e:
                raise e
        conn.commit()

    # Insert Query
    def insert(self, sql_file:str=None):
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            with open(sql_file, 'r') as f:
                insert_query = f.read()
                cursor.executescript(insert_query)
                logger.info(insert_query)    
                conn.commit()
        except Exception as e:
            self.close_connection()
            raise e

    # Select query
    def select(self, table_name:str=None, columns:str=None, where_clause:str=None, sql_file:str=None):
        conn = self.get_connection()
        cursor = conn.cursor()
        if sql_file:
            try:
                with open(sql_file, 'r') as f:
                    select_query = f.read()
                    logger.info(select_query) 
                    cursor.executescript(select_query)
            except Exception as e:
                raise e    
        else:
            columns_str = ', '.join(columns)
            select_query = f'SELECT {columns_str} FROM {table_name}'
            if where_clause is not None:
                select_query += f' WHERE {where_clause}'
            logger.info(select_query)    
            cursor.execute(select_query)
        rows = cursor.fetchall()
        return rows


    def delete(self, table_name:str = None, filter_clause:str = None, sql_file:str = None):
        conn = self.get_connection()
        cursor = conn.cursor()

        if sql_file:
            try:
                with open(sql_file, 'r') as f:
                    delete_query = f.read()
                    cursor.executescript(delete_query)
            except Exception as e:
                raise e
        else:
            delete_query = f'DELETE FROM {table_name} '
            if filter_clause:
                delete_query += f'WHERE {filter_clause}'
            logger.info(delete_query)    
            cursor.execute(delete_query)
        
        conn.commit()

