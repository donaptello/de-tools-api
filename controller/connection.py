import time

from loguru import logger
from fastapi import APIRouter, Query, Depends
from fastapi.responses import JSONResponse
from services.connection import ConnectionService
from models.connections.connections_payload import Connection


app = APIRouter()


@app.get("")
def connections(
    name: str = Query(default=None),
    connection_svc: ConnectionService = Depends(),
): 
    start_time = time.time()
    results,_ = connection_svc.get_connection(name=name)
    return JSONResponse(
        status_code=200,
        content={
            "statusCode": 200,
            "messages": "success",
            "timeExecution": time.time() - start_time,
            "data": results
        }
    )

@app.post("")
def insert_connections(
    connection_model: Connection,
    connection_svc: ConnectionService = Depends(),
): 
    start_time = time.time()
    connection_svc.insert_data(connection_model.dict())
    return JSONResponse(
        status_code=200,
        content={
            "statusCode": 201,
            "messages": "Inserted",
            "timeExecution": time.time() - start_time,
        }
    )

@app.put("/{id}")
def update_connections(
    id: str,
    connection_model: Connection,
    connection_svc: ConnectionService = Depends(),
): 
    start_time = time.time()
    connection_svc.update_data(id, connection_model.dict())
    return JSONResponse(
        status_code=200,
        content={
            "statusCode": 201,
            "messages": "Updated",
            "timeExecution": time.time() - start_time,
        }
    )

@app.delete("/{id}")
def delete_connections(
    id: str,
    connection_svc: ConnectionService = Depends(),
): 
    start_time = time.time()
    count = connection_svc.delete_data(id=id)
    return JSONResponse(
        status_code=200,
        content={
            "statusCode": 201,
            "messages": "Delete",
            "timeExecution": time.time() - start_time,
            "data": {
                "foundDelete": count
            }
        }
    )