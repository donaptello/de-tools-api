import pandas as pd
import sqlite3
import json

from datetime import datetime
from config.base import settings
from config.pg_config import JdbcConfig
from sqlalchemy.dialects.postgresql import insert


class UsersService: 
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
                CREATE TABLE users (
                    id TEXT,
                    user_full_name TEXT,
                    username TEXT,
                    password TEXT,
                    role TEXT,
                    CONSTRAINT users_pk PRIMARY KEY (id)
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
            filters = f"user_full_name LIKE '%%{name}%% OR username'"
        df = pd.read_sql(
            f"SELECT * FROM users WHERE {filters} ORDER BY created_at DESC",
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
        data['created_at'] = int(datetime.now().timestamp())
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
        return df.to_dict('records')[0]
    