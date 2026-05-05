import os 

from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI
from fastapi.responses import RedirectResponse, HTMLResponse
from services.connection import ConnectionService
from config.base import settings
from services.users import UsersService
from contextlib import asynccontextmanager
from loguru import logger
from controller import (
    hop,
    monitoring,
    connection,
    users,
    login
)
 
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting API")

    logger.info("Checking directory resources is exists?")
    if not os.path.exists('./resources'): 
        logger.info("Not found directory ./resources, process create...")
        os.makedirs('./resources')
        logger.info("Directory ./resources created!")

    logger.info("Creating sqlite if not exists")
    logger.info("Path location sqlite: {}", settings.SQLITE_DB)
    connect_svc = ConnectionService()
    connect_svc.create_table()
    users_svc = UsersService()
    users_svc.create_table()
    yield 
    logger.info("Shutting Down API")
 
app = FastAPI(
    title="API Data Engineer Tools",
    description="",
    version="0.0.1",
    lifespan=lifespan
)
app.add_middleware(
    CORSMiddleware,
    allow_methods=["*"],
    allow_headers=["*"],
    allow_origins=["*"]
)
 
@app.get('/', include_in_schema=False)
def redirect():
    return RedirectResponse('/docs')

 
@app.get("/rapidoc", response_class=HTMLResponse, include_in_schema=False)
async def rapidoc():
    return f"""
    <!doctype html>
    <html>
        <head>
            <meta charset="utf-8">
            <script 
                type="module" 
                src="https://unpkg.com/rapidoc/dist/rapidoc-min.js"
            ></script>
        </head>
        <body>
            <rapi-doc spec-url="{app.openapi_url}"></rapi-doc>
        </body> 
    </html>
    """
 
app.include_router(hop.app, prefix='/v1/hop', tags=['Hop Service'])
app.include_router(connection.app, prefix='/v1/connection', tags=['Connection Service'])
app.include_router(monitoring.app, prefix='/v1/monitoring', tags=['Monitoring Service'])
app.include_router(users.app, prefix='/v1/users', tags=['Users Management Service'])
app.include_router(login.app, prefix='/v1/auth', tags=['Auth Service'])

# if __name__ == "__main__":
#     uvicorn.run("main:app", host="127.0.0.1", port=8256, reload=True)