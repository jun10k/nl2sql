from os import statvfs
import pandas as pd
from fastapi import UploadFile
from bizops.services.postgres import PostgresService
from bizops.services.vector import VectorService

class DBController:
    def __init__(self):
        self.postgres_service = PostgresService()
        self.vector_service = VectorService()

    def process_database_metadata(self, file: UploadFile, database_name: str) -> None:
        """Process and store database metadata"""
        try:
            # Read CSV file
            df = pd.read_csv(file.file)
            
            # Update PostgreSQL database
            self.postgres_service.update_database_info(df, database_name)
            
            # Update vector database
            self.vector_service.update_database_info(df, database_name)
        except Exception as e:
            raise Exception(f"Failed to process database metadata: {str(e)}")
        finally:
            file.file.close()

    def process_table_metadata(self, file: UploadFile, table_name: str) -> None:
        """Process and store table metadata"""
        try:
            # Read CSV file
            df = pd.read_csv(file.file)
            
            # Update PostgreSQL database
            self.postgres_service.update_table_info(df, table_name)
            
            # Update vector database
            self.vector_service.update_table_info(df, table_name)
        except Exception as e:
            raise Exception(f"Failed to process table metadata: {str(e)}")
        finally:
            file.file.close()

    def process_query_examples(self, file: UploadFile, table_name: str) -> None:
        """Process and store sample data"""
        try:
            # Read CSV file
            df = pd.read_csv(file.file)
            
            # Update PostgreSQL database
            self.postgres_service.update_sample_data(df, table_name)
            
            # Update vector database
            self.vector_service.update_sample_vectors(df, table_name)
        except Exception as e:
            raise Exception(f"Failed to process sample data: {str(e)}")
        finally:
            file.file.close()

    def list_databases(self) -> list[str]:
        """List all databases"""
        try:
            return self.postgres_service.list_databases()
        except Exception as e:
            raise Exception(f"Failed to list databases: {str(e)}")

    def list_tables(self, database_name: str) -> list[str]:
        """List all tables in a database"""
        try:
            return self.postgres_service.list_tables(database_name)
        except Exception as e:
            raise Exception(f"Failed to list tables: {str(e)}")

    def get_table_details(self, database_name: str, table_name: str) -> list[dict]:
        """Get table details for a specific table in a database"""
        try:
            return self.postgres_service.get_table_details(database_name, table_name)
        except Exception as e:
            raise Exception(f"Failed to get table details: {str(e)}")

    def list_query_examples(self, database_name: str, table_name: str) -> list[dict]:
        """List query examples"""
        try:
            return self.vector_service.list_query_examples(database_name, table_name)
        except Exception as e:
            raise Exception(f"Failed to list query examples: {str(e)}")

    def update_query_examples(self, database_name: str, items: list) -> None:
        """Update query examples from JSON data"""
        try:
            # Update PostgreSQL database
            self.postgres_service.update_query_examples(database_name, items)
            
            # Update vector database
            self.vector_service.update_query_examples(database_name, items)
        except Exception as e:
            raise Exception(f"Failed to update query examples: {str(e)}")

    def update_table_info(self, database_name: str, items: list) -> None:
        """Update table information from JSON data"""
        try:
            # Convert items list to DataFrame
            df = pd.DataFrame(items)
            
            # Update PostgreSQL database
            self.postgres_service.update_table_info(df, database_name)
            
            # Update vector database
            self.vector_service.update_table_info(df, database_name)
        except Exception as e:
            raise Exception(f"Failed to update table information: {str(e)}")

    def update_database_info(self, database_name: str, items: list) -> None:
        """Update database information from JSON data"""
        try:
            # Ensure items is a list of dictionaries before converting to DataFrame
            if not items or not isinstance(items[0], dict):
                raise ValueError("Items must be a list of dictionaries")
                
            # Convert list to DataFrame
            df = pd.DataFrame(items)
            
            # Update PostgreSQL database
            self.postgres_service.update_database_info(df, database_name)
            
            # Update vector database
            self.vector_service.update_database_info(df, database_name)
        except Exception as e:
            raise Exception(f"Failed to update database information: {str(e)}")

    def update_table_details(self, database_name: str, table_name: str, items: list) -> None:
        """Update table details information from JSON data"""
        try:
            # Ensure items is a list of dictionaries before converting to DataFrame
            if not items or not isinstance(items[0], dict):
                raise ValueError("Items must be a list of dictionaries")
                
            # Convert list to DataFrame
            df = pd.DataFrame(items)
            
            # Update PostgreSQL database
            self.postgres_service.update_table_details(df, database_name, table_name)
            
            # Update vector database
            self.vector_service.update_table_details(df, database_name, table_name)
        except Exception as e:
            raise Exception(f"Failed to update table details: {str(e)}")
