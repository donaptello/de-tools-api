from fastapi import (
    APIRouter, 
    Depends
)
from fastapi.responses import JSONResponse
from services.v2.monitoring_service import MonitoringService


app = APIRouter()


@app.get("/list-table")
def list_table(
    search: str = None,
    monitoring_svc: MonitoringService = Depends()
): 
    results = monitoring_svc.get_list_table(search)
    return JSONResponse(
        status_code=200,
        content={
            "statusCode": 200,
            "messages": "success",
            "data": results
        }
    )


@app.get("")
def monitoring(): 
    pass