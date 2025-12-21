from pydantic import BaseModel
from constants.connection import ConnectionType

class Connection(BaseModel): 
    name: str
    type: ConnectionType
    configuration: dict
