import sqlite3

class Database:
    @staticmethod
    def create_connection(db:str):
        Database.conn = sqlite3.connect(db)
    
    def __init__(self):
        self.a = 5
        

obj = Database()        
print(obj.a)
Database.create_connection("b")
print(Database.conn) 