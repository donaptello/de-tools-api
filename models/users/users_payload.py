from pydantic import BaseModel
from enum import Enum


class Roles(Enum): 
    admin: str = "ADMIN"
    user: str = "USER"
    
class UserPayload(BaseModel): 
    user_full_name: str
    username: str
    password: str
    role: Roles
