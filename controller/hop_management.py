from fastapi import (
    APIRouter,
    Depends,
    Query
)
from fastapi.responses import JSONResponse
from services.hop_management import HopManagementService

app = APIRouter()


@app.get('/management/directory')
def directory_pipeline(
    search: str = None,
    hop_mangement_svc: HopManagementService = Depends()
):
    if search is not None: 
        results = hop_mangement_svc.find(search)
        return JSONResponse(
            status_code=200,
            content={
                "statusCode": 200,
                "messages": "success",
                "data": results
            }
        )
    results = hop_mangement_svc.find_all()
    return JSONResponse(
        status_code=200,
        content={
            "statusCode": 200,
            "messages": "success",
            "data": results
        }
    )

@app.get('/management/file')
def read_pipeline(
    path: str = Query(),
    hop_mangement_svc: HopManagementService = Depends()
): 
    results = hop_mangement_svc.read_file(path)
    return JSONResponse(
        status_code=200,
        content={
            "statusCode": 200,
            "messages": "success",
            "data": results
        }
    )
