from fastapi import FastAPI
from app.routers import example_router, file_upload, nl2sql

app = FastAPI()

app.include_router(example_router.router, prefix="/api/v1")
app.include_router(file_upload.router, prefix="/api/v1")
app.include_router(nl2sql.router, prefix="/api/v1")

@app.get("/")
async def read_root():
    return {"Hello": "World"}