from fastapi import FastAPI
from bizops.routers import symantic_layer, nl2sql

app = FastAPI()

app.include_router(symantic_layer.router, prefix="/api/v1")
# app.include_router(nl2sql.router, prefix="/api/v1")

@app.get("/")
async def read_root():
    return {"Hello": "World"}