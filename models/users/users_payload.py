from pydantic import BaseModel
from enum import Enum


class Roles(Enum): 
    admin: str = "ADMIN"
    user: str = "USER"
    data_engineer: str = "DATA_ENGINEER"
    data_analyst: str = "DATA_ANALYST"
    
class UserPayload(BaseModel): 
    user_full_name: str
    username: str
    password: str
    role: Roles

class Login(BaseModel): 
    username: str
    password: str