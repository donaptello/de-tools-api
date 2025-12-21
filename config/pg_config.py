import psycopg2

from sqlalchemy import create_engine
from config.base import settings
from sqlalchemy import URL
 
 
class JdbcConfig:
    def __init__(self):
        self.__database = settings.POSTGRES_DB
        self.__host = settings.POSTGRES_HOST
        self.__port = settings.POSTGRES_PORT
        self.__user = settings.POSTGRES_USER
        self.__pass = settings.POSTGRES_PASS
        self.__sqlite = settings.SQLITE_DB

    def client_connect_psycopg(self):
        conn = psycopg2.connect(
            database=self.__database,
            host=self.__host,
            user=self.__user,
            password=self.__pass,
            port=self.__port
        )
        return conn
    
    def client_sqlite(self): 
        engine = create_engine(
            f"sqlite:///{self.__sqlite}"
        )
        return engine.connect()
 
    def client_connect(self):
        url_object = URL.create(
            "postgresql",
            username=self.__user,
            password=self.__pass,
            host=self.__host,
            database=self.__database,
            port=self.__port
        )
        db = create_engine(url_object)
        return db.connect()
    
    def client_oracle(self): 
        url_object = URL.create(
            "oracle+oracledb",
            username=self.__user,
            password=self.__pass,
            host=self.__host,
            database=self.__database,
            port=self.__port
        )
        db = create_engine(url_object)
        return db.connect()