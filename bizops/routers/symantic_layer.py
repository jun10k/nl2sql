from fastapi import APIRouter, UploadFile, File, Body
from fastapi.responses import JSONResponse
from starlette import status
from typing import Optional, List, Dict, Union
from pydantic import BaseModel

from bizops.controller.db import DBController
from bizops.controller.file import FileController

router = APIRouter(
    prefix="/symantic-layer",
    tags=["symantic-layer"]
)

db_controller = DBController()
file_controller = FileController()

# Pydantic models for request validation
class DatabaseInfo(BaseModel):
    database_name: str
    aliases: List[str]
    description: str
    key_words: List[str]

class TableInfo(BaseModel):
    database_name: str
    table_name: str
    aliases: List[str]
    description: str
    ddl: str
    key_words: List[str]

class TableDetails(BaseModel):
    database_name: str
    table_name: str
    field_name: str
    data_type: str
    aliases: List[str]
    description: str
    key_words: List[str]

class QueryExample(BaseModel):
    database_name: List[str]
    query: List[str]
    description: str
    key_words: List[str]

class UpdateRequest(BaseModel):
    items: Union[List[DatabaseInfo], List[TableInfo], List[QueryExample], List[TableDetails]]

@router.post("/database/{database_name}/update/database-info", status_code=status.HTTP_201_CREATED)
def update_database_info(
    database_name: str,
    file: Optional[UploadFile] = File(None),
    data: Optional[UpdateRequest] = Body(None)
) -> JSONResponse:
    """
    Update database information via CSV file (prefixed with 'db_') or JSON object
    """
    try:
        if file:
            # Validate file name and extension
            if not file.filename:
                raise ValueError("Filename is required")
            if not file.filename.startswith("db_info_"):
                return JSONResponse(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    content={"detail": "File must be prefixed with 'db_' for database information upload"}
                )
            # Process the file
            database_name = file.filename.replace("db_", "").replace(".csv", "")
            result = db_controller.process_database_metadata(file, database_name)
        elif data:
            result = db_controller.update_database_info(database_name, data.items)
        else:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"detail": "Either file or data must be provided"}
            )
            
        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content=result
        )
        
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": f"Failed to update database info: {str(e)}"}
        )

@router.post("/database/{database_name}/update/table-info", status_code=status.HTTP_201_CREATED)
def update_table_info(
    database_name: str,
    file: Optional[UploadFile] = File(None),
    data: Optional[UpdateRequest] = Body(None)
) -> JSONResponse:
    """
    Update table information via CSV file (prefixed with 'tb_') or JSON object
    """
    try:
        if file:
            # Validate file name and extension
            if not file.filename:
                raise ValueError("Filename is required")
            if not file.filename.startswith("tb_info_"):
                return JSONResponse(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    content={"detail": "File must be prefixed with 'tb_' for table information upload"}
                )
            # Process the file
            table_name = file.filename.replace("tb_", "").replace(".csv", "")
            result = db_controller.process_table_metadata(file, table_name)
        elif data:
            result = db_controller.update_table_info(database_name, data.items)
        else:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"detail": "Either file or data must be provided"}
            )
            
        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content=result
        )
        
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": f"Failed to update table info: {str(e)}"}
        )

@router.post("/database/{database_name}/update/query-examples", status_code=status.HTTP_201_CREATED)
def update_query_examples(
    database_name: str,
    file: Optional[UploadFile] = File(None),
    data: Optional[UpdateRequest] = Body(None)
) -> JSONResponse:
    """
    Update query examples via CSV file (prefixed with 'sample_') or JSON object
    """
    try:
        if file:
            # Validate file name and extension
            if not file.filename:
                raise ValueError("Filename is required")
            if not file.filename.startswith("query_examples_"):
                return JSONResponse(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    content={"detail": "File must be prefixed with 'sample_' for query examples upload"}
                )
            # Process the file
            table_name = file.filename.replace("sample_", "").replace(".csv", "")
            result = db_controller.process_query_examples(file, table_name)
        elif data:
            result = db_controller.update_query_examples(database_name, data.items)
        else:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"detail": "Either file or data must be provided"}
            )
            
        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content=result
        )
        
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": f"Failed to update query examples: {str(e)}"}
        )

@router.post("/database/{database_name}/table/{table_name}/update/table-details", status_code=status.HTTP_201_CREATED)
def update_table_details(
    database_name: str,
    table_name: str,
    file: Optional[UploadFile] = File(None),
    data: Optional[UpdateRequest] = Body(None)
) -> JSONResponse:
    """
    Update table details information via CSV file (prefixed with 'tb_details_') or JSON object
    """
    try:
        if file:
            if not file.filename:
                raise ValueError("Filename is required")
            if not file.filename.startswith('tb_details_'):
                return JSONResponse(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    content={"message": "File name must start with 'tb_details_'"}
                )
            items = file_controller.process_csv_file(file, TableDetails)
            db_controller.update_table_details(database_name, table_name, items)
        elif data:
            db_controller.update_table_details(database_name, table_name, data.items)
        else:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"message": "Either file or data must be provided"}
            )
        
        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content={"message": "Table details updated successfully"}
        )
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"message": str(e)}
        )

@router.get("/database/{database_name}/table/{table_name}/table-details")
def list_table_details(database_name: str, table_name: str) -> JSONResponse:
    """
    List all table details for a specific table in a database
    """
    try:
        table_details = db_controller.get_table_details(database_name, table_name)
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"table_details": table_details}
        )
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"message": str(e)}
        )

@router.get("/databases", response_model=List[str])
def list_databases() -> List[str]:
    """List all databases in symantic layer"""
    try:
        return db_controller.list_databases()
    except Exception as e:
        raise Exception(
            f"Failed to list databases: {str(e)}"
        )

@router.get("/database/{database_name}/tables", response_model=List[str])
def list_tables(database_name: str) -> List[str]:
    """List all tables in specific database"""
    try:
        return db_controller.list_tables(database_name)
    except Exception as e:
        raise Exception(
            f"Failed to list tables: {str(e)}"
        )

@router.get("/database/{database_name}/query-examples")
def list_query_examples(
    database_name: str,
    table_name: str
) -> List[Dict]:
    """List sample queries for a database or specific table"""
    try:
        return db_controller.list_query_examples(database_name, table_name)
    except Exception as e:
        raise Exception(
            f"Failed to list query examples: {str(e)}"
        )