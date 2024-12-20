import pandas as pd
from typing import List, Dict, Any
from pgvector.sqlalchemy import Vector
from sqlalchemy import create_engine, text
from bizops.config import settings
from bizops.services.embedding import EmbeddingService
from llama_index.vector_stores import PGVectorStore
from llama_index.schema import Document

class VectorService:
    def __init__(self):
        self.engine = create_engine(settings.POSTGRES_URL)
        self.embedding_service = EmbeddingService()
        self.vector_store = PGVectorStore.from_engine(
            self.engine,
            table_name="document_vectors",
            embed_dim=1536  # Using OpenAI's embedding dimension
        )

    def update_database_info(self, df: pd.DataFrame, database_name: str) -> None:
        """Update database metadata vectors in PostgreSQL using PGVectorStore"""
        try:
            # Generate embeddings for the database info
            embeddings = self.embedding_service.process_database_info(df, database_name)
            
            # Create documents with metadata
            documents = []
            for (_, row), embedding in zip(df.iterrows(), embeddings):
                metadata = {
                    "database_name": database_name,
                    "table_name": row["table_name"],
                    "column_name": row["column_name"],
                    "description": row["description"],
                    "type": "database_metadata"
                }
                doc = Document(
                    text=f"{row['table_name']}.{row['column_name']}: {row['description']}",
                    metadata=metadata,
                    embedding=embedding
                )
                documents.append(doc)
            
            # Add documents to vector store
            self.vector_store.add_documents(documents)
            
        except Exception as e:
            raise Exception(f"Failed to update database vectors: {str(e)}")

    def update_table_info(self, df: pd.DataFrame, table_name: str) -> None:
        """Update table metadata vectors in PostgreSQL using PGVectorStore"""
        try:
            # Generate embeddings for the metadata descriptions
            descriptions = df['description'].tolist()
            embeddings = self.embedding_service.get_embeddings(descriptions)
            
            # Create documents with metadata
            documents = []
            for (_, row), embedding in zip(df.iterrows(), embeddings):
                metadata = {
                    "table_name": table_name,
                    "column_name": row["column_name"],
                    "description": row["description"],
                    "type": "table_metadata"
                }
                doc = Document(
                    text=f"{table_name}.{row['column_name']}: {row['description']}",
                    metadata=metadata,
                    embedding=embedding
                )
                documents.append(doc)
            
            # Add documents to vector store
            self.vector_store.add_documents(documents)
            
        except Exception as e:
            raise Exception(f"Failed to update table vectors: {str(e)}")

    def update_sample_vectors(self, df: pd.DataFrame, table_name: str) -> None:
        """Update sample data vectors in PostgreSQL using PGVectorStore"""
        try:
            # Generate embeddings for each row
            sample_texts = df.apply(lambda row: ' '.join(str(val) for val in row), axis=1).tolist()
            embeddings = self.embedding_service.get_embeddings(sample_texts)
            
            # Create documents with metadata
            documents = []
            for text, embedding in zip(sample_texts, embeddings):
                metadata = {
                    "table_name": table_name,
                    "type": "sample_data"
                }
                doc = Document(
                    text=text,
                    metadata=metadata,
                    embedding=embedding
                )
                documents.append(doc)
            
            # Add documents to vector store
            self.vector_store.add_documents(documents)
            
        except Exception as e:
            raise Exception(f"Failed to update sample vectors: {str(e)}")

    def update_query_examples(self, database_name: str, items: List[Dict[str, Any]]) -> None:
        """Update query example vectors in PostgreSQL using PGVectorStore"""
        try:
            # Convert items to DataFrame
            df = pd.DataFrame(items)
            
            # Generate embeddings for the queries
            queries = df['query'].tolist()
            embeddings = self.embedding_service.get_embeddings(queries)
            
            # Create documents with metadata
            documents = []
            for item, embedding in zip(items, embeddings):
                metadata = {
                    "database_name": database_name,
                    "query": item["query"],
                    "description": item.get("description", ""),
                    "type": "query_example"
                }
                doc = Document(
                    text=f"Query: {item['query']}\nDescription: {item.get('description', '')}",
                    metadata=metadata,
                    embedding=embedding
                )
                documents.append(doc)
            
            # Add documents to vector store
            self.vector_store.add_documents(documents)
            
        except Exception as e:
            raise Exception(f"Failed to update query examples: {str(e)}")

    def search_similar_documents(self, query: str, filter_metadata: Dict[str, Any] = None, limit: int = 5) -> List[Dict[str, Any]]:
        """Search for similar documents in the vector store"""
        try:
            # Get embedding for the query
            query_embedding = self.embedding_service.get_embeddings([query])[0]
            
            # Search in vector store
            results = self.vector_store.similarity_search_with_score(
                query_embedding,
                k=limit,
                filter_dict=filter_metadata
            )
            
            # Format results
            formatted_results = []
            for doc, score in results:
                formatted_results.append({
                    "text": doc.text,
                    "metadata": doc.metadata,
                    "score": score
                })
            
            return formatted_results
            
        except Exception as e:
            raise Exception(f"Failed to search similar documents: {str(e)}")

    def list_query_examples(self, database_name: str, table_name: str = None) -> List[Dict[str, Any]]:
        """List query examples for a database or specific table"""
        try:
            # Filter by database name and table name
            filter_metadata = {
                "database_name": database_name
            }
            if table_name:
                filter_metadata["table_name"] = table_name
            
            # Get documents from vector store
            documents = self.vector_store.get_documents(filter_dict=filter_metadata)
            
            # Format results
            formatted_results = []
            for doc in documents:
                formatted_results.append({
                    "text": doc.text,
                    "metadata": doc.metadata
                })
            
            return formatted_results
            
        except Exception as e:
            raise Exception(f"Failed to list query examples: {str(e)}")

    def update_table_details(self, df: pd.DataFrame, database_name: str, table_name: str) -> None:
        """Update table details vectors in PostgreSQL using PGVectorStore"""
        try:
            # Generate embeddings for the descriptions
            descriptions = df['description'].tolist()
            embeddings = self.embedding_service.get_embeddings(descriptions)
            
            # Create documents with metadata
            documents = []
            for (_, row), embedding in zip(df.iterrows(), embeddings):
                metadata = {
                    "database_name": database_name,
                    "table_name": table_name,
                    "field_name": row["field_name"],
                    "data_type": row["data_type"],
                    "aliases": row["aliases"],
                    "description": row["description"],
                    "keywords": row["keywords"],
                    "type": "table_details"
                }
                doc = Document(
                    text=f"{table_name}.{row['field_name']}: {row['description']}",
                    metadata=metadata,
                    embedding=embedding
                )
                documents.append(doc)
            
            # Add documents to vector store
            self.vector_store.add_documents(documents)
            
        except Exception as e:
            raise Exception(f"Failed to update table details vectors: {str(e)}")
