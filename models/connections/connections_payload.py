from pydantic import BaseModel, Field
from constants.connection import ConnectionType

class Connection(BaseModel): 
    name: str
    type: ConnectionType
    description: str = Field(default="")
    configuration: dict
