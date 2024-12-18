import json
import os
from typing import List, Dict, Any

import pandas as pd
from fastapi import HTTPException
from llama_index.core import Document
from llama_index.vector_stores.postgres import PGVectorStore
from sqlalchemy import create_engine

from bizops.pkg.models import ModelManager, EmbeddingType


class EmbeddingService:
    _embedding_type : EmbeddingType = EmbeddingType.AZURE_EMBEDDING
    
    def __init__(self):
        # Initialize model manager
        self.model_manager = ModelManager()
        
        # Get embedding model from model manager
        self.embed_model = self.model_manager.get_embedding_model(self._embedding_type)
        
        # Initialize PostgreSQL connection
        self.connection_string = os.getenv(
            "POSTGRES_CONNECTION",
            "postgresql://postgres:postgres@localhost:5432/vector_store"
        )
        self.engine = create_engine(self.connection_string)
        
        # Initialize PG Vector Store
        self.vector_store = PGVectorStore.from_params(
            database=self.connection_string,
            host="localhost",
            port="5432",
            user="postgres",
            password="postgres",
            table_name="embeddings",
            embed_dim=EmbeddingType.AZURE_DIMENSION.value
        )

    def process_database_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process database information and generate embeddings
        
        Args:
            df: DataFrame containing database metadata
            
        Returns:
            DataFrame with embeddings added
        """
        try:
            if "embedding" not in df.columns:
                df["embedding"] = None  # Initialize the column
            # Process each record individually
            for idx, row in df.iterrows():
                text_parts = []
                
                # Add each field to text parts if it exists
                if 'database_name' in row and row['database_name']:
                    text_parts.append(f"Database name: {row['database_name']}")
                
                if 'aliases' in row and row['aliases']:
                    text_parts.append(f"Aliases: {row['aliases']}")
                
                if 'description' in row and row['description']:
                    text_parts.append(f"Description: {row['description']}")
                
                if 'keywords' in row and row['keywords']:
                    text_parts.append(f"Keywords: {row['keywords']}")
                
                # Combine all text parts and generate embedding
                combined_text = "\n".join(text_parts)
                embedding = self.embed_model.get_text_embedding(combined_text)
                df.at[idx, "embedding"] = embedding
            
            return df

        except Exception as e:
            raise Exception(f"Failed to process database info: {str(e)}")

    def process_table_file(self, file_path: str, database_name: str) -> Dict[str, Any]:
        """Process table description CSV file"""
        try:
            df = pd.read_csv(file_path)
            required_columns = {'table_name', 'column_name', 'aliases', 'data_type', 'description', 'keywords'}
            if not all(col in df.columns for col in required_columns):
                raise HTTPException(
                    status_code=400,
                    detail=f"Table CSV must contain columns: {required_columns}"
                )

            # Group by table_name
            tables_processed = 0
            for table_name, table_df in df.groupby('table_name'):
                # Create table schema description
                table_text = f"Table '{table_name}' in database '{database_name}' has the following columns:"
                column_texts = []
                
                for _, row in table_df.iterrows():
                    col_text = f"Column '{row['column_name']}' with type {row['data_type']}"
                    if pd.notna(row['description']):
                        col_text += f": {row['description']}"
                    column_texts.append(col_text)

                # Create embeddings for table schema
                self._create_embeddings(
                    texts=[table_text] + column_texts,
                    metadata={
                        "database": database_name,
                        "table": table_name,
                        "type": "table_schema"
                    }
                )
                tables_processed += 1

            return {
                "status": "success",
                "database": database_name,
                "tables_processed": tables_processed,
                "columns_processed": len(df)
            }

        except HTTPException as he:
            raise he
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to process table description file: {str(e)}"
            )

    def process_sample_file(self, file_path: str, database_name: str) -> Dict[str, Any]:
        """Process sample SQL statements CSV file"""
        try:
            df = pd.read_csv(file_path)
            required_columns = {'table_name', 'query', 'description'}
            if not all(col in df.columns for col in required_columns):
                raise HTTPException(
                    status_code=400,
                    detail=f"Sample SQL CSV must contain columns: {required_columns}"
                )

            # Process each sample query
            texts = []
            for _, row in df.iterrows():
                # Create natural language description of the query
                text = f"For table '{row['table_name']}' in database '{database_name}': {row['description']}"
                texts.append(text)

                # Store the actual SQL query in metadata
                metadata = {
                    "database": database_name,
                    "table": row['table_name'],
                    "type": "sample_query",
                    "sql_query": row['query']
                }

                # Create embedding for this sample
                self._create_embeddings(
                    texts=[text],
                    metadata=metadata
                )

            return {
                "status": "success",
                "database": database_name,
                "samples_processed": len(df)
            }

        except HTTPException as he:
            raise he
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to process sample SQL file: {str(e)}"
            )

    def _create_embeddings(self, texts: List[str], metadata: Dict) -> None:
        """Create embeddings and store in PostgreSQL vector store"""
        try:
            # Validate input
            if not texts:
                raise ValueError("texts parameter cannot be empty")
                
            # Create Document objects with metadata
            documents = []
            for text in texts:
                doc = Document(
                    text=text,
                    metadata=metadata
                )
                documents.append(doc)
            
            # Create embeddings using llama_index
            embeddings = self.embed_model.get_text_embedding_batch([doc.text for doc in documents])
            
            # Store in PostgreSQL
            for doc, embedding in zip(documents, embeddings):
                self.vector_store.add(
                    embedding=embedding,
                    doc_id=str(hash(doc.text)),
                    metadata=json.dumps(doc.metadata)
                )
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to create embeddings: {str(e)}"
            )

    def search_similar(self, query: str, metadata_filter: Dict = None, top_k: int = 5) -> List[Dict]:
        """Search for similar embeddings"""
        try:
            # Get query embedding
            query_embedding = self.embed_model.get_text_embedding(query)
            
            # Search in vector store
            results = self.vector_store.similarity_search(
                query_embedding,
                k=top_k,
                metadata_filter=metadata_filter
            )
            
            return [
                {
                    "text": result[0].text,
                    "metadata": json.loads(result[0].metadata),
                    "score": result[1]
                }
                for result in results
            ]
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to search embeddings: {str(e)}"
            )