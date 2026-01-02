import json
import sqlite3
import pandas as pd 
import hashlib

from datetime import datetime
from config.base import settings
from config.pg_config import JdbcConfig
from sqlalchemy.dialects.postgresql import insert


class ConnectionService: 
    def __init__(self):
        self.__db: str = settings.SQLITE_DB
        self.__jdbc_obj: JdbcConfig = JdbcConfig()

    def __connect(self):
        return sqlite3.connect(self.__db)
    
    def __hashing_id(self, name: str): 
        dt_obj = int(datetime.now().timestamp() * 1000)
        hash_id = hashlib.md5(f"{name}-{dt_obj}".encode('utf-8')).hexdigest()
        return hash_id
    
    def create_table(self): 
        conn = self.__connect()
        cursor = conn.cursor()
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS connections (
                    id TEXT,
                    name TEXT,
                    configuration TEXT,
                    type TEXT,
                    CONSTRAINT connections_pk PRIMARY KEY (id)
                )
            """
        )
        conn.commit()
        cursor.close()
        conn.close()

    def get_connection(self, name: str): 
        conn = self.__jdbc_obj.client_sqlite()
        filters = "1+1"
        if name is not None: 
            filters = f"name LIKE '%%{name}%%'"
        df = pd.read_sql(
            f"SELECT * FROM connections WHERE {filters}",
            con=conn
        )
        conn.connection.close()
        df['configuration'] = df['configuration'].apply(lambda row: json.loads(row))
        return df.to_dict("records"), df.shape[0]
    
    def insert_on_conflict_nothing(self, table, conn, keys, data_iter):
        data = [dict(zip(keys, row)) for row in data_iter]
        insert_statement = insert(table.table).values(data)
        conflict_update = insert_statement.on_conflict_do_update(
            constraint=f"id",
            set_={column.key: column for column in insert_statement.excluded},
        )
        result = conn.execute(conflict_update)
        return result.rowcount

    def insert_data(self, data: dict): 
        data['id'] = self.__hashing_id(data['name'])
        data['type'] = data['type'].value
        data['configuration'] = json.dumps(data['configuration'])
        df = pd.DataFrame([data])

        conn = self.__jdbc_obj.client_sqlite()
        df.to_sql(
            'connections',
            con=conn,
            index=False,
            if_exists='append',
            method=self.insert_on_conflict_nothing
        )
        conn.connection.close()

    def update_data(self, id: str, data: dict): 
        data['id'] = id
        data['type'] = data['type'].value
        data['configuration'] = json.dumps(data['configuration'])
        df = pd.DataFrame([data])
        conn = self.__jdbc_obj.client_sqlite()
        df.to_sql(
            'connections',
            con=conn,
            index=False,
            if_exists='append',
            method=self.insert_on_conflict_nothing
        )
        conn.connection.close()

    def delete_data(self, id: str): 
        conn = self.__connect()
        cursor = conn.cursor()

        cursor.execute(f"DELETE FROM connections WHERE id = '{id}';")
        result = cursor.rowcount

        conn.commit()
        cursor.close()
        conn.close()
        return result


