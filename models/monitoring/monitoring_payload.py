from pydantic import BaseModel, Field
from typing import Optional
from enum import Enum


class Layer(Enum): 
    all_layer: str = "all-layer"
    bronze: str = "bronze"
    silver: str = "silver"
    gold: str = "gold"

class Flag(Enum): 
    source: str = "source"
    target: str = "target"


class MonitoringParameterPayload(BaseModel): 
    table_name_source: str = Field(alias="tableNameSource")
    schemas: Optional[str] = Field(alias="schemas")
    db_source: str = Field(alias="dbSource")
    db_target: str = Field(alias="dbTarget")
    column_date_name: Optional[str] = Field(alias="columnDateName")
    table_name_target: str = Field(alias="tableNameTarget")
    data_source_column_name: str = Field(alias="dataSourceColumnName")
    data_source: Optional[str] = Field(alias="dataSource") 
    layer: Layer = Field(default=Layer.bronze)
    flag: Flag