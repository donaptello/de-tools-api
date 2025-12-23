from fastapi import APIRouter, Query
from services.hop_service import HopService
from fastapi.responses import JSONResponse
from constants.hop import HopMode
 
app = APIRouter()
 
@app.get('/pipeline-log')
def get_pipeline_log(
    mode: HopMode = Query(default=HopMode.all)
):
    hop_service = HopService(mode.value)
    results = hop_service.get_pipeline()
    return JSONResponse(
        status_code=200,
        content={
            "statusCode": 200,
            "messages": "success",
            "data": results
        }
    )
 
@app.delete('/pipeline-log')
def delete_pipeline_log(
    mode: HopMode = Query(default=HopMode.all),
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