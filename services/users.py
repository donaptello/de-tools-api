import pandas as pd
import sqlite3
import hashlib

from datetime import datetime
from config.base import settings
from config.pg_config import JdbcConfig
from sqlalchemy.dialects.postgresql import insert
from services.auth_service import AuthService


class UsersService: 
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
                CREATE TABLE IF NOT EXISTS users (
                    id TEXT,
                    user_full_name TEXT,
                    username TEXT,
                    password TEXT,
                    role TEXT,
                    created_at NUMBER,
                    CONSTRAINT users_pk PRIMARY KEY (id)
                )
            """
        )
        conn.commit()
        cursor.close()
        conn.close()

    def get_users(self, name: str): 
        conn = self.__jdbc_obj.client_sqlite()
        filters = "1+1"
        if name is not None: 
            filters = f"user_full_name LIKE '%%{name}%%' OR username LIKE '%%{name}%%'"
        df = pd.read_sql(
            f"SELECT * FROM users WHERE {filters} ORDER BY created_at DESC",
            con=conn
        )
        df.drop(columns=["password"], inplace=True)
        conn.connection.close()
        return df.to_dict("records"), df.shape[0]
    
    def find_one_user(self, username: str): 
        conn = self.__jdbc_obj.client_sqlite()
        df = pd.read_sql(
            f"SELECT id, username, password, role FROM users WHERE username = '{username}'",
            con=conn
        )
        conn.connection.close()
        
        if df.shape[0] == 0:
            return None
        return df.to_dict('records')[0]

    
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
        data['id'] = self.__hashing_id(data['username'])
        data['role'] = data['role'].value
        data['created_at'] = int(datetime.now().timestamp())
        auth_service = AuthService()
        data['password'] = auth_service.hash_password(data['password'])
        df = pd.DataFrame([data])

        conn = self.__jdbc_obj.client_sqlite()
        df.to_sql(
            'users',
            con=conn,
            index=False,
            if_exists='append',
            method=self.insert_on_conflict_nothing
        )
        conn.connection.close()
        return df.to_dict('records')[0]
    
    def update_data(self, id: str, data: dict):
        data['id'] = id
        data['role'] = data['role'].value

        conn = self.__jdbc_obj.client_sqlite()
        df = pd.DataFrame([data])
        df.to_sql(
            'users',
            con=conn,
            index=False,
            if_exists='append',
            method=self.insert_on_conflict_nothing
        )
        conn.connection.close()
        return data

    def delete_data(self, id: str): 
        conn = self.__connect()
        cursor = conn.cursor()

        cursor.execute(f"DELETE FROM users WHERE id = '{id}';")
        result = cursor.rowcount

        conn.commit()
        cursor.close()
        conn.close()
        return result
    