from fastapi import APIRouter, Query
from services.hop_service import HopService
from helpers.hop_helpers import filter_hop, parse_date
from fastapi.responses import JSONResponse
from constants.hop import (
    HopMode, 
    StatusHop, 
    Orders, 
    OrdersBy,
    OptionsMode
)
 
app = APIRouter()

@app.get('/status')
def hop_status(): 
    hop_service = HopService()
    results = hop_service.get_status()
    return JSONResponse(
        status_code=200,
        content={
            "statusCode": 200,
            "messages": "success",
            "data": results
        }
    )
 
@app.get('/orchestration/{mode}')
def pipeline_log(
    mode: HopMode = HopMode.all,
    id_pipe: str = Query(default=None),
    name_pipe: str = Query(default=None),
    search_name: str = Query(default=None),
    status: StatusHop = StatusHop.all,
    order: Orders = Orders.desc,
    orderBy: OrdersBy = OrdersBy.startDate,
    size: int = Query(default=10)
):
    hop_service = HopService(mode=mode.value)
    results = hop_service.get_pipeline_v2(id_pipe, name_pipe, search_name)
    if id_pipe is None: 
        results = filter_hop(status.value, results)
        
        if orderBy.value == "startDate":
            results = sorted(results, key=parse_date, reverse=False if order.value == "asc" else True)
        else: 
            results = sorted(results, key=lambda x: x[orderBy.value], reverse=False if order.value == "asc" else True)
    return JSONResponse(
        status_code=200,
        content={
            "statusCode": 200,
            "messages": "success",
            "data": results[:size]
        }
    )

@app.get('/orchestration/{mode}/{options}')
def options_mode(
    mode: HopMode = HopMode.all,
    options: OptionsMode = OptionsMode.none,
    id_pipe: str = Query(default=None),
    name_pipe: str = Query(default=None),
): 
    result = None
    if options.value is None: 
        return JSONResponse(
            status_code=401,
            content={
                "statusCode": 401,
                "messages": "options must be input",
                "data": None
            }
        )
    hop_service = HopService(mode=mode.value)
    if options.value == "start": 
        result = hop_service.start_pipeline(id_pipe, name_pipe)
    elif options.value == "stop":
        result = hop_service.stop_pipeline(id_pipe, name_pipe)
    elif options.value == "remove": 
        result = hop_service.remove_pipeline(id_pipe, name_pipe)
    else: 
        return JSONResponse(
            status_code=404,
            content={
                "statusCode": 404,
                "messages": "options not found",
                "data": None
            }
        )

    if result is None: 
        return JSONResponse(
            status_code=404,
            content={
                "statusCode": 404,
                "messages": "success",
                "data": {
                    "id": id_pipe,
                    "name": name_pipe,
                    "status": f"success {options.value}"
                }
            }
        )
    return JSONResponse(
        status_code=200,
        content={
            "statusCode": 200,
            "messages": "success",
            "data": {
                "id": id_pipe,
                "name": name_pipe,
                "status": f"success {options.value}"
            }
        }
    )
 
@app.delete('/orchestration/{mode}')
def pipeline_log(
    mode: HopMode = HopMode.all,
    with_error: bool = False
):
    hop_service = HopService(mode.value)
    results = hop_service.delete_pipeline(with_error)
    return JSONResponse(
        status_code=200,
        content={
            "statusCode": 200,
            "messages": "delete",
            "data": {
                'totalDelete': len(results)
            }
        }
    )