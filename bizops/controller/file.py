import os
import shutil
from datetime import datetime
from typing import List, Dict, Literal, Type, TypeVar, Any
from fastapi import UploadFile
from bizops.services.embedding import EmbeddingService
import csv
import aiofiles

# Create uploads directory if it doesn't exist
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

FileType = Literal["database", "table", "sample"]

class FileController:
    def __init__(self):
        self.embedding_service = EmbeddingService()

    @staticmethod
    def generate_unique_filename(original_filename: str) -> str:
        """Generate a unique filename by adding timestamp"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        name, ext = os.path.splitext(original_filename)
        return f"{name}_{timestamp}{ext}"

    @staticmethod
    def identify_file_type(filename: str) -> FileType:
        """Identify the type of CSV file based on its prefix"""
        if filename.startswith("db_"):
            return "database"
        elif filename.startswith("tb_"):
            return "table"
        elif filename.startswith("sample_"):
            return "sample"
        else:
            raise ValueError("Invalid file prefix. File must start with 'db_', 'tb_', or 'sample_'")

    @staticmethod
    def validate_file_type(filename: str) -> None:
        """Validate if the file type is allowed"""
        allowed_extensions = {'.csv'}
        file_ext = os.path.splitext(filename)[1].lower()
        
        if file_ext not in allowed_extensions:
            raise ValueError(f"File type not allowed. Allowed types: {', '.join(allowed_extensions)}")

    @staticmethod
    async def save_file(file: UploadFile, file_path: str) -> None:
        """Save the uploaded file to disk"""
        try:
            # Use aiofiles for async file operations
            async with aiofiles.open(file_path, "wb") as buffer:
                content = await file.read()
                await buffer.write(content)
        except Exception as e:
            raise ValueError(f"Failed to save file: {str(e)}")

    async def process_csv_file(self, file: UploadFile, model_class: Type[Any]) -> List[Any]:
        """Process a CSV file and convert its contents to a list of model objects"""
        try:
            # Read the CSV file content asynchronously
            content = (await file.read()).decode('utf-8')
            await file.seek(0)  # Reset file pointer for potential future reads
            
            # Parse CSV content
            reader = csv.DictReader(content.splitlines())
            items = []
            
            for row in reader:
                # Clean up the row data - strip whitespace from keys and values
                cleaned_row = {k.strip(): v.strip() if isinstance(v, str) else v 
                             for k, v in row.items()}
                
                # Convert string lists to actual lists
                for key, value in cleaned_row.items():
                    if key in ['aliases', 'keywords', 'query']:
                        if value:
                            cleaned_row[key] = [item.strip() for item in value.split(',')]
                        else:
                            cleaned_row[key] = []
                
                # Create model instance
                item = model_class(**cleaned_row)
                items.append(item)
            
            return items
        except Exception as e:
            raise ValueError(f"Failed to process CSV file: {str(e)}")

    async def handle_file_upload(self, file: UploadFile, database_name: str) -> Dict:
        """Handle the complete file upload process"""
        if not database_name:
            raise ValueError("Database name is required")
            
        # Validate file name and extension
        if not file.filename:
            raise ValueError("Filename is required")
        
        # Validate file extension
        self.validate_file_type(file.filename)
        
        # Identify file type from prefix
        file_type = self.identify_file_type(file.filename)
        
        # Generate unique filename
        unique_filename = self.generate_unique_filename(file.filename)
        file_path = os.path.join(UPLOAD_DIR, unique_filename)
        
        # Save the file
        await self.save_file(file, file_path)
        
        # Process the file based on its type
        if file_type == "database":
            embedding_result = self.embedding_service.process_database_file(
                file_path=file_path,
                database_name=database_name
            )
        elif file_type == "table":
            embedding_result = self.embedding_service.process_table_file(
                file_path=file_path,
                database_name=database_name
            )
        else:  # sample
            embedding_result = self.embedding_service.process_sample_file(
                file_path=file_path,
                database_name=database_name
            )
        
        return {
            "status": "success",
            "message": f"{file_type.capitalize()} file uploaded and processed successfully",
            "filename": unique_filename,
            "original_filename": file.filename,
            "file_type": file_type,
            "embedding_details": embedding_result
        }