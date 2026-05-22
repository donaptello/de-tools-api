from pydantic import BaseSettings


class Settings(BaseSettings): 
    POSTGRES_HOST: str
    POSTGRES_PORT: int
    POSTGRES_USER: str
    POSTGRES_PASS: str
    POSTGRES_DB: str
    POSTGRES_SCHEMA_MONITORING: str

    APACHE_HOP_HOST: str
    APACHE_HOP_PORT: str
    APACHE_HOP_USER: str
    APACHE_HOP_PASS: str 
    APACHE_HOP_DIRECTORY_PROJECT: str

    API_METHOD: str 
    API_HEADER: str
    API_ORIGINS: str

    SQLITE_DB: str
    SECRET_KEY: str

    TESTING_API: bool
    
    class Config: 
        env_file = '.env'

settings = Settings()