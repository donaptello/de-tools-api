from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI
from fastapi.responses import RedirectResponse, HTMLResponse
from services.connection import ConnectionService
from contextlib import asynccontextmanager
from loguru import logger
from controller import (
    hop,
    monitoring,
    connection
)
 
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting API")
    logger.info("Creating sqlite if not exists")
    connect_svc = ConnectionService()
    connect_svc.create_table()
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

# if __name__ == "__main__":
#     uvicorn.run("main:app", host="127.0.0.1", port=8256, reload=True)