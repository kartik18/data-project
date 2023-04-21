from utils.logging import get_logger
import sqlite3

logger = get_logger(__name__)

class Database:
    _obj = None

    def __new__(cls, db_name):
        if cls._obj is None:
            cls._obj = super(Database, cls).__new__(cls)
            cls._obj.db_name = db_name
            cls._obj.connection = None
        logger.info("Sending new connection")
        return cls._obj

    def get_connection(self):
        #if self.connection is None:
        self.connection = sqlite3.connect(self.db_name)
        return self.connection

    def close_connection(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def create_table(self, table_name=None, columns=None, sql_file=None):
        conn = self.get_connection()
        cursor = conn.cursor()

        if sql_file:
            with open(sql_file, 'r') as f:
                create_query = f.read()
                cursor.executescript(create_query)
        else:
            columns_str = ', '.join([f'{column} {columns[column]}' for column in columns])
            create_query = f'CREATE TABLE IF NOT EXISTS {table_name} ({columns_str})'
            cursor.execute(create_query)
        
        conn.commit()

    def insert(self, table_name=None, data=None, sql_file=None):
        conn = self.get_connection()
        cursor = conn.cursor()

        if sql_file:
             with open(sql_file, 'r') as f:
                insert_query = f.read()
                cursor.executescript(insert_query)
        else:        
            placeholders = ', '.join(['?' for _ in data])
            insert_query = f'INSERT INTO {table_name} VALUES ({placeholders})'
            cursor.execute(insert_query, data)
        conn.commit()
        conn.close()

    def select(self, table_name=None, columns=None, where_clause=None, sql_file=None):
        conn = self.get_connection()
        cursor = conn.cursor()
        

        
        columns_str = ', '.join(columns)
        select_query = f'SELECT {columns_str} FROM {table_name}'
        if where_clause is not None:
            select_query += f' WHERE {where_clause}'

        logger.info(select_query)
        cursor.execute(select_query)
        rows = cursor.fetchall()

        return rows

    def update(self, table_name, data, where_clause):
        conn = self.get_connection()
        c = conn.cursor()

        set_str = ', '.join([f'{column}=?' for column in data])
        update_query = f'UPDATE {table_name} SET {set_str} WHERE {where_clause}'

        c.execute(update_query, data)
        conn.commit()

    def delete(self, table_name = None, filter_clause = None, sql_file = None):
        conn = self.get_connection()
        cursor = conn.cursor()

        if sql_file:
            with open(sql_file, 'r') as f:
                delete_query = f.read()
                cursor.executescript(delete_query)
        else:
            delete_query = f'DELETE FROM {table_name} '
            if filter_clause:
                delete_query += f'WHERE {filter_clause}'
            logger.info(delete_query)    
            cursor.execute(delete_query)
        
        conn.commit()

