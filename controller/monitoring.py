import time

from fastapi import APIRouter, Query, Depends
from services.monitoring_service import MonitoringService
from fastapi.responses import JSONResponse
from constants.monitoring import WidgetEnum

app = APIRouter()

@app.get('')
def monitoring(
    monitoring_obj: MonitoringService = Depends(),
    table_name: str = Query(
        alias="table",
        default=None
    ),
): 
    start_time = time.time()
    results = monitoring_obj.get_monitoring(
        name=table_name
    )

    return JSONResponse(
        status_code=200,
        content={
            "statusCode": 200,
            "messages": "success",
            "timeExecution": time.time() - start_time,
            "data": results,
        }
    )

@app.get('/widget/{type}')
def widget(
    type: WidgetEnum = WidgetEnum.TOTAL_CARD,
    monitoring_obj: MonitoringService = Depends(),
): 
    start_time = time.time()
    results = []
    if type.value == "total_card":
        results = monitoring_obj.get_total_widget()
        
    return JSONResponse(
        status_code=200,
        content={
            "statusCode": 200,
            "messages": "success",
            "timeExecution": time.time() - start_time,
            "data": results,
        }
    )


@app.get('/detail/{table_name}')
def monitoring_detail(
    table_name: str,
    monitoring_obj: MonitoringService = Depends()
): 
    start_time = time.time()
    results, total_data = monitoring_obj.get_monitoring_detail(
        name=table_name
    )
    return JSONResponse(
        status_code=200,
        content={
            "statusCode": 200,
            "messages": "success",
            "timeExecution": time.time() - start_time,
            "data": results,
        }
    )

