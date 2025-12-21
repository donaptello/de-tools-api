import sqlite3
import pandas as pd 

from config.base import settings
from config.pg_config import JdbcConfig


class ConnectionService: 
    def __init__(self):
        self.__db: str = settings.SQLITE_DB
        self.__jdbc_obj: JdbcConfig = JdbcConfig()

    def __connect(self):
        return sqlite3.connect(self.__db)
    
    def create_table(self): 
        conn = self.__connect()
        cursor = conn.cursor()
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS connections (
                    id TEXT,
                    name TEXT,
                    configuration TEXT,
                    type TEXT
                )
            """
        )
        conn.commit()
        cursor.close()
        conn.close()

    def get_connection(self, name: str): 
        conn = self.__jdbc_obj.client_sqlite()
        df = pd.read_sql(
            f"SELECT * FROM connections WHERE name LIKE '%%{name}%%'",
            con=conn
        )
        conn.connection.close()
        return df.to_dict("records"), df.shape[0]

    def insert_data(self, data: dict): 
        df = pd.DataFrame([])

