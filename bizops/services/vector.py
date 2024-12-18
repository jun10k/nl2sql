import pandas as pd
from typing import List, Dict, Any
from pgvector.sqlalchemy import Vector
from sqlalchemy import create_engine, text
from bizops.config import settings
from bizops.services.embedding import EmbeddingService

class VectorService:
    def __init__(self):
        self.engine = create_engine(settings.POSTGRES_URL)
        self.embedding_service = EmbeddingService()

    def update_database_info(self, df: pd.DataFrame, database_name: str) -> None:
        """Update database metadata vectors in PostgreSQL"""
        try:
            # Generate embeddings for the database info
            embeddings = self.embedding_service.process_database_info(df, database_name)
            
            # Add embeddings to the DataFrame
            df['embedding'] = [Vector(emb) for emb in embeddings]
            
            # Create temporary table for the new data
            temp_table = f"temp_vector_{database_name}"
            df.to_sql(temp_table, self.engine, if_exists='replace', index=False)
            
            # Merge data into the main table
            with self.engine.connect() as conn:
                conn.execute(text(f"""
                    INSERT INTO database_metadata_vectors 
                    (database_name, table_name, column_name, description, embedding)
                    SELECT database_name, table_name, column_name, description, embedding
                    FROM {temp_table}
                    ON CONFLICT (database_name, table_name, column_name)
                    DO UPDATE SET
                        description = EXCLUDED.description,
                        embedding = EXCLUDED.embedding
                """))
                
                # Drop temporary table
                conn.execute(text(f"DROP TABLE {temp_table}"))
                conn.commit()
        except Exception as e:
            raise Exception(f"Failed to update database vectors: {str(e)}")

    def update_table_info(self, df: pd.DataFrame, table_name: str) -> None:
        """Update table metadata vectors in PostgreSQL"""
        try:
            # Generate embeddings for the metadata descriptions
            descriptions = df['description'].tolist()
            embeddings = self.embedding_service.get_embeddings(descriptions)
            
            # Add embeddings to the DataFrame
            df['embedding'] = [Vector(emb) for emb in embeddings]
            
            # Create temporary table for the new data
            temp_table = f"temp_vector_{table_name}"
            df.to_sql(temp_table, self.engine, if_exists='replace', index=False)
            
            # Merge data into the main table
            with self.engine.connect() as conn:
                conn.execute(text(f"""
                    INSERT INTO table_metadata_vectors 
                    (table_name, column_name, description, embedding)
                    SELECT table_name, column_name, description, embedding
                    FROM {temp_table}
                    ON CONFLICT (table_name, column_name)
                    DO UPDATE SET
                        description = EXCLUDED.description,
                        embedding = EXCLUDED.embedding
                """))
                
                # Drop temporary table
                conn.execute(text(f"DROP TABLE {temp_table}"))
                conn.commit()
        except Exception as e:
            raise Exception(f"Failed to update table vectors: {str(e)}")

    def update_sample_vectors(self, df: pd.DataFrame, table_name: str) -> None:
        """Update sample data vectors in PostgreSQL"""
        try:
            # Generate embeddings for each row
            sample_texts = df.apply(lambda row: ' '.join(str(val) for val in row), axis=1).tolist()
            embeddings = self.embedding_service.get_embeddings(sample_texts)
            
            # Add embeddings to the DataFrame
            df['embedding'] = [Vector(emb) for emb in embeddings]
            
            # Create or replace the sample vectors table
            df.to_sql(f"sample_vectors_{table_name}", self.engine, if_exists='replace', index=False)
        except Exception as e:
            raise Exception(f"Failed to update sample vectors: {str(e)}")

    def update_query_examples(self, database_name: str, items: list) -> None:
        """Update query example vectors in PostgreSQL"""
        try:
            # Convert items to DataFrame
            df = pd.DataFrame(items)
            
            # Generate embeddings for the queries
            queries = df['query'].tolist()
            embeddings = self.embedding_service.get_embeddings(queries)
            
            # Add embeddings to the DataFrame
            df['embedding'] = [Vector(emb) for emb in embeddings]
            
            # Create temporary table for the new data
            temp_table = f"temp_vector_queries_{database_name}"
            df.to_sql(temp_table, self.engine, if_exists='replace', index=False)
            
            # Merge data into the main table
            with self.engine.connect() as conn:
                conn.execute(text(f"""
                    INSERT INTO query_examples 
                    (database_name, table_name, query, description, embedding)
                    SELECT database_name, table_name, query, description, embedding
                    FROM {temp_table}
                    ON CONFLICT (database_name, table_name, query)
                    DO UPDATE SET
                        description = EXCLUDED.description,
                        embedding = EXCLUDED.embedding
                """))
                
                # Drop temporary table
                conn.execute(text(f"DROP TABLE {temp_table}"))
                conn.commit()
        except Exception as e:
            raise Exception(f"Failed to update query vectors: {str(e)}")

    def list_query_examples(self, database_name: str, table_name: str = None) -> List[Dict[str, Any]]:
        """List query examples for a database or specific table"""
        try:
            with self.engine.connect() as conn:
                if table_name:
                    query = text("""
                        SELECT * FROM query_examples 
                        WHERE database_name = :database_name 
                        AND table_name = :table_name
                    """)
                    result = conn.execute(query, {"database_name": database_name, "table_name": table_name})
                else:
                    query = text("""
                        SELECT * FROM query_examples 
                        WHERE database_name = :database_name
                    """)
                    result = conn.execute(query, {"database_name": database_name})
                
                return [dict(row) for row in result]
        except Exception as e:
            raise Exception(f"Failed to list query examples: {str(e)}")

    def update_table_details(self, df: pd.DataFrame, database_name: str, table_name: str) -> None:
        """Update table details vectors in PostgreSQL"""
        try:
            # Generate embeddings for the descriptions
            descriptions = df['description'].tolist()
            embeddings = self.embedding_service.get_embeddings(descriptions)
            
            # Add embeddings to the DataFrame
            df['embedding'] = [Vector(emb) for emb in embeddings]
            
            # Create temporary table for the new data
            temp_table = f"temp_table_details_vectors_{database_name}_{table_name}"
            df.to_sql(temp_table, self.engine, if_exists='replace', index=False)
            
            # Merge data into the main table
            with self.engine.connect() as conn:
                conn.execute(text(f"""
                    INSERT INTO table_details_vectors 
                    (database_name, table_name, field_name, data_type, aliases, description, keywords, embedding)
                    SELECT :database_name as database_name, :table_name as table_name,
                           field_name, data_type, aliases, description, keywords, embedding
                    FROM {temp_table}
                    ON CONFLICT (database_name, table_name, field_name)
                    DO UPDATE SET
                        data_type = EXCLUDED.data_type,
                        aliases = EXCLUDED.aliases,
                        description = EXCLUDED.description,
                        keywords = EXCLUDED.keywords,
                        embedding = EXCLUDED.embedding
                """), {"database_name": database_name, "table_name": table_name})
                
                # Drop temporary table
                conn.execute(text(f"DROP TABLE {temp_table}"))
                conn.commit()
        except Exception as e:
            raise Exception(f"Failed to update table details vectors: {str(e)}")
