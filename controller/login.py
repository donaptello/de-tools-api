from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from services.auth_service import AuthService
from services.users import UsersService
from fastapi.exceptions import HTTPException
from fastapi.security.oauth2 import OAuth2PasswordRequestForm
from loguru import logger

app = APIRouter()

@app.post("/login")
def login(
    login_data: OAuth2PasswordRequestForm = Depends(),
    user_svc: UsersService = Depends(),
    auth_svc: AuthService = Depends()
): 
    user_data = user_svc.find_one_user(login_data.username)
    if not user_data: 
        return JSONResponse(
            status_code=404,
            content={
                'statusCode': 404,
                "messages": "Username not found"
            }
        )
    
    if not(auth_svc.verify_password(login_data.password, user_data["password"])):
        return JSONResponse(
            status_code=401,
            content={
                'statusCode': 401,
                "messages": "Invalid Password"
            }
        )
    
    token = auth_svc.create_token(
        data={
            "_id": user_data["id"], 
            "_role": user_data['role'],
            "_username": user_data['username'],
        }
    )
    return JSONResponse(
        status_code=201,
        content={
            "access_token": token
        }
    )