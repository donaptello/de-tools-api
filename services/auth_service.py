from passlib.context import CryptContext
from config.base import settings


class AuthService: 
    def __init__(self):
        self.__secret_key: str = settings.SECRET_KEY
        self.__algoritm: str = "HS256"
        self.__context = CryptContext(schemes=["bcrypt"], deprecated="auto")

    def __hash_password(self, password: str): 
        return self.__context.hash(password)
    
    def __verify_password(self, plain_password, hashed_password): 
        return self.__context.verify(plain_password, hashed_password)
    
    def create_token(self, data: dict): 
        pass

    def verify_token(self, token: str): 
        pass

    def get_current_user(self): 
        pass