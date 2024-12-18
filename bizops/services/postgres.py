import pandas as pd
from sqlalchemy import create_engine, text, ARRAY, String
from typing import List, Dict, Any
from bizops.config import settings
from bizops.services.embedding import EmbeddingService


class PostgresService:
    def __init__(self):
        self.engine = create_engine(settings.POSTGRES_URL)
        self.embedding_service = EmbeddingService()
        self._ensure_tables_exist()
    
    def _ensure_tables_exist(self):
        """Ensure all required tables exist in the database"""
        with self.engine.connect() as conn:
            # Ensure pgvector extension is installed
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))

            # Create database_info table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS database_info (
                    database_name VARCHAR(255) PRIMARY KEY,
                    aliases TEXT[],
                    description TEXT,
                    keywords TEXT[],
                    embedding VECTOR(1536)
                )
            """))
            
            # Create table_info table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS table_info (
                    database_name VARCHAR(255),
                    table_name VARCHAR(255),
                    aliases TEXT[],
                    description TEXT,
                    ddl TEXT,
                    keywords TEXT[],
                    embedding VECTOR(1536),
                    PRIMARY KEY (database_name, table_name)
                )
            """))
            
            # Create table_details table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS table_details (
                    database_name VARCHAR(255),
                    table_name VARCHAR(255),
                    field_name VARCHAR(255),
                    data_type VARCHAR(255),
                    aliases TEXT[],
                    description TEXT,
                    keywords TEXT[],
                    embedding VECTOR(1536),
                    PRIMARY KEY (database_name, table_name, field_name)
                )
            """))
            
            # Create query_examples table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS query_examples (
                    id SERIAL PRIMARY KEY,
                    database_name TEXT[],
                    query TEXT,
                    description TEXT,
                    keywords TEXT[],
                    embedding VECTOR(1536)
                )
            """))
            conn.commit()

    def update_database_info(self, df: pd.DataFrame) -> None:
        """Update database information in PostgreSQL"""
        try:
            temp_table = "temp_database_info"
            # Add embeddings to the DataFrame
            df = self.embedding_service.process_database_info(df)

            # Convert string to list, then to PostgreSQL array format
            df['aliases'] = df['aliases'].apply(lambda x: [item.strip() for item in x.split(',')])
            df['keywords'] = df['keywords'].apply(lambda x: [item.strip() for item in x.split(',')])
            
            with self.engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
                conn.commit()
                
                # Specify the correct data types for array columns
                dtype = {
                    'aliases': ARRAY(String),
                    'keywords': ARRAY(String),
                }
                df.to_sql(temp_table, conn, if_exists='replace', index=False, dtype=dtype)
                
                conn.execute(text(f"""
                    INSERT INTO database_info (database_name, aliases, description, keywords, embedding)
                    SELECT database_name, aliases, description, keywords, embedding
                    FROM {temp_table}
                    ON CONFLICT (database_name)
                    DO UPDATE SET
                        aliases = EXCLUDED.aliases,
                        description = EXCLUDED.description,
                        keywords = EXCLUDED.keywords,
                        embedding = EXCLUDED.embedding
                """))
                
                conn.execute(text(f"DROP TABLE {temp_table}"))
                conn.commit()
        except Exception as e:
            raise Exception(f"Failed to update database info: {str(e)}")

    def update_table_info(self, df: pd.DataFrame) -> None:
        """Update table information in PostgreSQL"""
        try:
            temp_table = "temp_table_info"
            
            with self.engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
                conn.commit()
                
                # Specify the correct data types for array columns
                dtype = {
                    'aliases': ARRAY(String),
                    'keywords': ARRAY(String)
                }
                df.to_sql(temp_table, conn, if_exists='replace', index=False, dtype=dtype)
                
                conn.execute(text(f"""
                    INSERT INTO table_info (
                        database_name, table_name, aliases, description, ddl, keywords
                    )
                    SELECT 
                        database_name, table_name, aliases, description, ddl, keywords
                    FROM {temp_table}
                    ON CONFLICT (database_name, table_name)
                    DO UPDATE SET
                        aliases = EXCLUDED.aliases,
                        description = EXCLUDED.description,
                        ddl = EXCLUDED.ddl,
                        keywords = EXCLUDED.keywords
                """))
                
                conn.execute(text(f"DROP TABLE {temp_table}"))
                conn.commit()
        except Exception as e:
            raise Exception(f"Failed to update table info: {str(e)}")

    def update_table_details(self, df: pd.DataFrame) -> None:
        """Update table details in PostgreSQL"""
        try:
            temp_table = "temp_table_details"
            
            with self.engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
                conn.commit()
                
                # Specify the correct data types for array columns
                dtype = {
                    'aliases': ARRAY(String),
                    'keywords': ARRAY(String)
                }
                df.to_sql(temp_table, conn, if_exists='replace', index=False, dtype=dtype)
                
                conn.execute(text(f"""
                    INSERT INTO table_details (
                        database_name, table_name, field_name, data_type, 
                        aliases, description, keywords
                    )
                    SELECT 
                        database_name, table_name, field_name, data_type,
                        aliases, description, keywords
                    FROM {temp_table}
                    ON CONFLICT (database_name, table_name, field_name)
                    DO UPDATE SET
                        data_type = EXCLUDED.data_type,
                        aliases = EXCLUDED.aliases,
                        description = EXCLUDED.description,
                        keywords = EXCLUDED.keywords
                """))
                
                conn.execute(text(f"DROP TABLE {temp_table}"))
                conn.commit()
        except Exception as e:
            raise Exception(f"Failed to update table details: {str(e)}")

    def update_query_examples(self, df: pd.DataFrame) -> None:
        """Update query examples in PostgreSQL"""
        try:
            temp_table = "temp_query_examples"
            
            with self.engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
                conn.commit()
                
                # Specify the correct data types for array columns
                dtype = {
                    'database_name': ARRAY(String),
                    'query': ARRAY(String),
                    'keywords': ARRAY(String)
                }
                df.to_sql(temp_table, conn, if_exists='replace', index=False, dtype=dtype)
                
                conn.execute(text("""
                    INSERT INTO query_examples (database_name, query, description, keywords)
                    SELECT database_name, query, description, keywords
                    FROM temp_query_examples
                """))
                
                conn.execute(text(f"DROP TABLE {temp_table}"))
                conn.commit()
        except Exception as e:
            raise Exception(f"Failed to update query examples: {str(e)}")

    def list_databases(self) -> List[str]:
        """List all databases"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT database_name FROM database_info"))
                return [row[0] for row in result]
        except Exception as e:
            raise Exception(f"Failed to list databases: {str(e)}")

    def list_tables(self, database_name: str) -> List[str]:
        """List all tables in a database"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT table_name 
                    FROM table_info 
                    WHERE database_name = :database_name
                """), {"database_name": database_name})
                return [row[0] for row in result]
        except Exception as e:
            raise Exception(f"Failed to list tables: {str(e)}")

    def get_table_details(self, database_name: str, table_name: str) -> List[Dict[str, Any]]:
        """Get table details for a specific table"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT * FROM table_details 
                    WHERE database_name = :database_name 
                    AND table_name = :table_name
                """), {"database_name": database_name, "table_name": table_name})
                return [dict(row) for row in result]
        except Exception as e:
            raise Exception(f"Failed to get table details: {str(e)}")
