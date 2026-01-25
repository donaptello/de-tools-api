from pydantic import BaseModel, Field
from typing import Optional, List
from enum import Enum

class Flag(Enum): 
    silver: str = "silver"
    bronze: str = "bronze"
    gold: str = "gold"


class MonitoringTable(BaseModel): 
    tableName: str = Field(alias="table_name")
    lastRunEtl: str = Field(alias="lastrun")
    lastUpdateData: str = Field(alias="date")
    CodeSource: str = Field(alias="data_source", default=None)
    DbSource: str = Field(alias="source", default=None)
    DbTarget: str = Field(alias="target", default=None)
    RecordSource: Optional[int] = Field(alias="total_in_source")
    RecordDwh: Optional[int] = Field(alias="total_in_target")
    TotalDiffRecord: float = Field(alias="diff")
    status: str = Field(alias="status")

    class Config:
        allow_population_by_field_name = True

class MonitoringDetail(BaseModel):
    lastrun: str = Field(alias="lastrun")
    date: str = Field(alias="date")
    dataSource: Optional[str] = Field(alias="data_source")
    tableName: str = Field(alias="table_name") 
    source: Optional[str] = Field(alias="source")
    target: Optional[str] = Field(alias="target")
    totalInSource: Optional[int] = Field(alias="total_in_source")
    totalInTarget: Optional[int] = Field(alias="total_in_target")
    diff: float = Field(alias="diff")

    class Config:
        allow_population_by_field_name = True

class MonitoringParameterDetailResponse(BaseModel): 
    id: int
    tableNameSource: str = Field(alias="table_name_source")
    schemas: Optional[str] = Field(alias="schema")
    dbSource: str = Field(alias="db_source")
    dbTarget: str = Field(alias="db_target")
    columnDateName: Optional[str] = Field(alias="column_date_name")
    tableNameTarget: str = Field(alias="table_name_target")
    dataSourceColumnName: Optional[str] = Field(alias="data_source_column_name")
    dataSource: Optional[str] = Field(alias="data_source")
    layer: str = Field(alias="layer")
    flag: str = Field(alias="flag")
    insertTime: str = Field(alias="insert_time") 

    class Config:
        allow_population_by_field_name = True

class MonitoringParameterResponse(BaseModel): 
    id: int
    tableNameSource: str = Field(alias="table_name_source")
    schemas: Optional[str] = Field(alias="schemas")
    dbSource: str = Field(alias="db_source")
    dbTarget: str = Field(alias="db_target")
    columnDateName: Optional[str] = Field(alias="column_date_name")
    tableNameTarget: str = Field(alias="table_name_target")
    dataSourceColumnName: Optional[str] = Field(alias="data_source_column_name")
    dataSource: Optional[str] = Field(alias="data_source")
    layer: str = Field(alias="layer")
    flag: str = Field(alias="flag")
    details: Optional[List[MonitoringParameterDetailResponse]] = Field(alias="details")
    insertTime: str = Field(alias="insert_time") 

    class Config:
        allow_population_by_field_name = True

