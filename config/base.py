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

    class Config: 
        env_file = '.env'

settings = Settings()