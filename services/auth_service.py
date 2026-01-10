from passlib.context import CryptContext
from fastapi.exceptions import HTTPException
from fastapi import status
from config.base import settings
from datetime import datetime, timezone, timedelta
from pydantic import BaseModel
from fastapi import Depends
from typing import Optional
from fastapi.security.oauth2 import OAuth2PasswordBearer
from jose import jwt, JWTError
import traceback

class TokenData(BaseModel):
    id: Optional[str] = None

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/v1/auth/login")


class AuthService: 
    def __init__(self):
        self.__secret_key: str = settings.SECRET_KEY
        self.__algoritm: str = "HS256"
        self.__context = CryptContext(schemes=["bcrypt"], deprecated="auto")
        self.__header_field = "WWW-Authenticate"

    def hash_password(self, password: str): 
        return self.__context.hash(password)
    
    def verify_password(self, plain_password, hashed_password): 
        return self.__context.verify(plain_password, hashed_password)
    
    def create_token(self, data: dict): 
        expire = datetime.now(timezone.utc) + timedelta(hours=6)
        data['exp'] = expire
        token = jwt.encode(data, self.__secret_key, algorithm=self.__algoritm)
        return token
    
    def response_failed(self, messages: str): 
        return HTTPException(
            status_code=401,
            detail={
                'statusCode': 401,
                'messages': messages
            },
            headers={self.__header_field: "Bearer"}
        )

    def verify_token(self, token: str): 
        try: 
            if not token:
                raise self.response_failed("Auth Header Is Empty")
            
            payload = jwt.decode(
                token, 
                self.__secret_key, 
                algorithms=self.__algoritm
            )
            
            id = payload.get("_id")
            token_data = TokenData(id=id)
            if not id:
                return self.response_failed("Could not validate credentials")
            return token_data, payload

        except JWTError as err: 
            traceback.print_exc()
            raise self.response_failed("Token Is Expired")

    def get_current_user(self, tokens:str = Depends(oauth2_scheme)): 
        _, user_data = self.verify_token(tokens)
        return user_data
