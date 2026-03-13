from fastapi import APIRouter, Query
from services.hop_service import HopService
from fastapi.responses import JSONResponse
from constants.hop import HopMode
 
app = APIRouter()

@app.get('/status')
def get_hop_status(): 
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
def get_pipeline_log(
    mode: HopMode = HopMode.all,
    id_pipe: str = Query(default=None)
):
    hop_service = HopService(mode=mode.value)
    results = hop_service.get_pipeline_v2(id_pipe)
    return JSONResponse(
        status_code=200,
        content={
            "statusCode": 200,
            "messages": "success",
            "data": results
        }
    )
 
@app.delete('/orchestration/{mode}')
def delete_pipeline_log(
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