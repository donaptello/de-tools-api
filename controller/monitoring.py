import time

from fastapi import APIRouter, Query, Depends
from services.monitoring_service import MonitoringService
from fastapi.responses import JSONResponse
from constants.monitoring import WidgetEnum
from models.monitoring.monitoring_response import MonitoringTable

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
    results_mapped = [MonitoringTable(**raw).dict() for raw in results]

    return JSONResponse(
        status_code=200,
        content={
            "statusCode": 200,
            "messages": "success",
            "timeExecution": time.time() - start_time,
            "data": results_mapped,
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
        result_mapped = {}
        for res in results: 
            if res['status'] == "incompleted": 
                result_mapped['inCompleted'] = res['total']
                continue
            if res['status'] == "completed":  
                result_mapped['completed'] = res['total']
                continue
            if res['status'] == "to_be_checked": 
                result_mapped['toBeChecked'] = res['total']
                continue
            if res['status'] == "total_table": 
                result_mapped['totalTable'] = res['total']
                continue

        
    return JSONResponse(
        status_code=200,
        content={
            "statusCode": 200,
            "messages": "success",
            "timeExecution": time.time() - start_time,
            "data": result_mapped,
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

