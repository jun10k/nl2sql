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
                    text_parts.append(f"Database Aliases: {row['aliases']}")
                
                if 'description' in row and row['description']:
                    text_parts.append(f"Database Description: {row['description']}")
                
                if 'keywords' in row and row['keywords']:
                    text_parts.append(f"Database Keywords: {row['keywords']}")
                
                # Combine all text parts and generate embedding
                combined_text = "\n".join(text_parts)
                embedding = self.embed_model.get_text_embedding(combined_text)
                df.at[idx, "embedding"] = embedding
            
            return df

        except Exception as e:
            raise Exception(f"Failed to process database info: {str(e)}")

    def process_table_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process table information and generate embeddings
        
        Args:
            df: DataFrame containing table metadata
            
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
                if 'table_name' in row and row['table_name']:
                    text_parts.append(f"Table name: {row['table_name']}")
                
                if 'aliases' in row and row['aliases']:
                    text_parts.append(f"Table Aliases: {row['aliases']}")
                
                if 'keywords' in row and row['keywords']:
                    text_parts.append(f"Table Keywords: {row['keywords']}")

                if 'description' in row and row['description']:
                    text_parts.append(f"Table Description: {row['description']}")
                
                # Combine all text parts and generate embedding
                combined_text = "\n".join(text_parts)
                embedding = self.embed_model.get_text_embedding(combined_text)
                df.at[idx, "embedding"] = embedding
            
            return df

        except Exception as e:
            raise Exception(f"Failed to process table info: {str(e)}")

    def process_table_details(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process table details and generate embeddings
        
        Args:
            df: DataFrame containing table metadata
            
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
                if 'field_name' in row and row['field_name']:
                    text_parts.append(f"Field Name: {row['field_name']}")

                if 'aliases' in row and row['aliases']:
                    text_parts.append(f"Field Aliases: {row['aliases']}")

                if 'keywords' in row and row['keywords']:
                    text_parts.append(f"Field Keywords: {row['keywords']}")

                if 'description' in row and row['description']:
                    text_parts.append(f"Field Description: {row['description']}")
                
                # Combine all text parts and generate embedding
                combined_text = "\n".join(text_parts)
                embedding = self.embed_model.get_text_embedding(combined_text)
                df.at[idx, "embedding"] = embedding
            
            return df

        except Exception as e:
            raise Exception(f"Failed to process table details: {str(e)}")

    def process_query_examples(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process query examples and generate embeddings
        
        Args:
            df: DataFrame containing query examples
            
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
                    text_parts.append(f"Database Name: {row['database_name']}")

                if 'keywords' in row and row['keywords']:
                    text_parts.append(f"Query Keywords: {row['keywords']}")
                
                if 'description' in row and row['description']:
                    text_parts.append(f"Query Description: {row['description']}")
                
                # Combine all text parts and generate embedding
                combined_text = "\n".join(text_parts)
                embedding = self.embed_model.get_text_embedding(combined_text)
                df.at[idx, "embedding"] = embedding
            
            return df

        except Exception as e:
            raise Exception(f"Failed to process query examples: {str(e)}")

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