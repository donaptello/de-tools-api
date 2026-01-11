import time

from fastapi.responses import JSONResponse
from fastapi import APIRouter, Query, Depends
from services.users import UsersService
from models.users.users_payload import UserPayload
from services.auth_service import AuthService
from loguru import logger

app = APIRouter()


@app.get("")
def get_users(
    search: str = Query(default=None),
    user_svc: UsersService = Depends(),
    current_users: dict = Depends(AuthService().get_current_user)
): 
    start_time = time.time()
    results,_ = user_svc.get_users(search)
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
def insert_users(
    users: UserPayload,
    user_svc: UsersService = Depends(),
    current_users: dict = Depends(AuthService().get_current_user)
): 
    start_time = time.time()
    res = user_svc.insert_data(users.dict())
    return JSONResponse(
        status_code=201,
        content={
            "statusCode": 201,
            "messages": "Inserted",
            "timeExecution": time.time() - start_time,
            "data": res
        }
    )

@app.put("/{id}")
def update_users(
    users: UserPayload,
    id: str,
    user_svc: UsersService = Depends(),
    current_users: dict = Depends(AuthService().get_current_user)
): 
    start_time = time.time()
    res = user_svc.update_data(id, users.dict())
    return JSONResponse(
        status_code=201,
        content={
            "statusCode": 201,
            "messages": "Updated",
            "timeExecution": time.time() - start_time,
            "data": res
        }
    )

@app.delete("/{id}")
def delete_users(
    id: str,
    user_svc: UsersService = Depends(),
    current_users: dict = Depends(AuthService().get_current_user)
): 
    start_time = time.time()
    res = user_svc.delete_data(id)
    return JSONResponse(
        status_code=201,
        content={
            "statusCode": 201,
            "messages": "Updated",
            "timeExecution": time.time() - start_time,
            "data": {
                "id": id,
                "foundDelete": res
            }
        }
    )