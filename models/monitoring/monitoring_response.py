from pydantic import BaseModel, Field
from typing import Optional


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