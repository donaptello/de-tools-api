import time

from fastapi import APIRouter, Query, Depends
from services.monitoring_service import MonitoringService
from fastapi.responses import JSONResponse
from constants.monitoring import WidgetEnum
from models.monitoring.monitoring_payload import MonitoringParameterPayload, Layer, Flag
from models.monitoring.monitoring_response import (
    MonitoringTable, 
    MonitoringDetail,
    MonitoringParameterResponse,
)

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
    limit: int = 100,
    monitoring_obj: MonitoringService = Depends()
): 
    start_time = time.time()
    results, total_data = monitoring_obj.get_monitoring_detail(
        name=table_name,
        limit=limit
    )
    results_mapped = []
    count_diff = 0
    for raw in results: 
        mapped_raw = MonitoringDetail(**raw).dict()
        results_mapped.append(mapped_raw)
        if raw['diff'] == 0: 
            continue
        count_diff += 1

    return JSONResponse(
        status_code=200,
        content={
            "statusCode": 200,
            "messages": "success",
            "timeExecution": time.time() - start_time,
            "data": {
                "countDiff": count_diff,
                "detail": results_mapped
            },
        }
    )

@app.get('/parameter')
def get_params_mapping(
    name: str = None,
    layer: Layer = Layer.bronze,
    flag: Flag = Flag.source,
    monitoring_obj: MonitoringService = Depends()
): 
    start_time = time.time()

    results = monitoring_obj.get_param_mapping(
        name=name, 
        flag=flag.value,
        layer=layer.value
    )
    result_mapped = [MonitoringParameterResponse(**res).dict() for res in results]

    return JSONResponse(
        status_code=200,
        content={
            "statusCode": 200,
            "messages": "success",
            "timeExecution": time.time() - start_time,
            "data": result_mapped,
        }
    )

@app.post('/parameter')
def insert_params_mapping(
    payload_model: MonitoringParameterPayload,
    monitoring_obj: MonitoringService = Depends()
): 
    start_time = time.time()
    result = monitoring_obj.insert_param_mapping(payload_model.dict())

    return JSONResponse(
        status_code=201,
        content={
            "statusCode": 201,
            "messages": "inserted",
            "timeExecution": time.time() - start_time,
            "data": result,
        }
    )

@app.put('/parameter')
def update_params_mapping(
    table_name: str,
    layer: Layer,
    flag: Flag,
    payload_model: MonitoringParameterPayload,
    monitoring_obj: MonitoringService = Depends()
): 
    start_time = time.time()
    result, row_updated = monitoring_obj.update_param_mapping(
        table_name=table_name,
        layer=layer.value,
        flag=flag.value,
        payload=payload_model.dict()
    )
    if not result: 
        return JSONResponse(
            status_code=404,
            content={
                "statusCode": 404,
                "messages": "not found",
                "timeExecution": time.time() - start_time,
                "data": result,
            }
        )
    
    return JSONResponse(
        status_code=201,
        content={
            "statusCode": 200,
            "messages": "updated",
            "timeExecution": time.time() - start_time,
            "data": {
                "data": result,
                "row_updated": row_updated
            },
        }
    )

@app.delete('/parameter')
def delete_params_mapping(
    table_name: str,
    layer: Layer,
    flag: Flag,
    monitoring_obj: MonitoringService = Depends()
): 
    start_time = time.time()
    result = monitoring_obj.delete_param_mapping(
        table_name=table_name,
        flag=flag.value,
        layer=layer.value
    )

    return JSONResponse(
        status_code=201,
        content={
            "statusCode": 201,
            "messages": "deleted",
            "timeExecution": time.time() - start_time,
            "data": {
                "deleted": result
            },
        }
    )

    