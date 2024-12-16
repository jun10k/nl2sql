import pandas as pd
from sqlalchemy import create_engine, text
from typing import List, Dict, Any
from bizops.config import settings

class PostgresService:
    def __init__(self):
        self.engine = create_engine(settings.POSTGRES_URL)

    def update_database_info(self, df: pd.DataFrame, database_name: str) -> None:
        """Update database metadata in PostgreSQL"""
        try:
            # Convert DataFrame to records for insertion
            records = df.to_dict('records')
            
            # Create temporary table for the new data
            temp_table = f"temp_{database_name}"
            df.to_sql(temp_table, self.engine, if_exists='replace', index=False)
            
            # Merge data into the main table
            with self.engine.connect() as conn:
                conn.execute(text(f"""
                    INSERT INTO database_metadata (database_name, table_name, column_name, data_type, description)
                    SELECT database_name, table_name, column_name, data_type, description
                    FROM {temp_table}
                    ON CONFLICT (database_name, table_name, column_name)
                    DO UPDATE SET
                        data_type = EXCLUDED.data_type,
                        description = EXCLUDED.description
                """))
                
                # Drop temporary table
                conn.execute(text(f"DROP TABLE {temp_table}"))
                conn.commit()
        except Exception as e:
            raise Exception(f"Failed to update database metadata: {str(e)}")

    def update_table_info(self, df: pd.DataFrame, table_name: str) -> None:
        """Update table metadata in PostgreSQL"""
        try:
            # Convert DataFrame to records for insertion
            records = df.to_dict('records')
            
            # Create temporary table for the new data
            temp_table = f"temp_{table_name}"
            df.to_sql(temp_table, self.engine, if_exists='replace', index=False)
            
            # Merge data into the main table
            with self.engine.connect() as conn:
                conn.execute(text(f"""
                    INSERT INTO table_metadata (table_name, column_name, data_type, description)
                    SELECT table_name, column_name, data_type, description
                    FROM {temp_table}
                    ON CONFLICT (table_name, column_name)
                    DO UPDATE SET
                        data_type = EXCLUDED.data_type,
                        description = EXCLUDED.description
                """))
                
                # Drop temporary table
                conn.execute(text(f"DROP TABLE {temp_table}"))
                conn.commit()
        except Exception as e:
            raise Exception(f"Failed to update table metadata: {str(e)}")

    def update_sample_data(self, df: pd.DataFrame, table_name: str) -> None:
        """Update sample data in PostgreSQL"""
        try:
            # Create or replace the sample data table
            df.to_sql(f"sample_{table_name}", self.engine, if_exists='replace', index=False)
        except Exception as e:
            raise Exception(f"Failed to update sample data: {str(e)}")

    def list_databases(self) -> List[str]:
        """List all databases from database_metadata table"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT DISTINCT database_name FROM database_metadata"))
                return [row[0] for row in result]
        except Exception as e:
            raise Exception(f"Failed to list databases: {str(e)}")

    def list_tables(self, database_name: str) -> List[str]:
        """List all tables in a database"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT DISTINCT table_name 
                    FROM database_metadata 
                    WHERE database_name = :database_name
                    ORDER BY table_name
                """), {"database_name": database_name})
                return [row[0] for row in result]
        except Exception as e:
            raise Exception(f"Failed to list tables: {str(e)}")

    def update_query_examples(self, database_name: str, items: list) -> None:
        """Update query examples in PostgreSQL"""
        try:
            # Convert items to DataFrame
            df = pd.DataFrame(items)
            
            # Create temporary table for the new data
            temp_table = f"temp_query_examples_{database_name}"
            df.to_sql(temp_table, self.engine, if_exists='replace', index=False)
            
            # Merge data into the main table
            with self.engine.connect() as conn:
                conn.execute(text(f"""
                    INSERT INTO query_examples (database_name, table_name, query, description)
                    SELECT :database_name as database_name, table_name, query, description
                    FROM {temp_table}
                    ON CONFLICT (database_name, table_name, query)
                    DO UPDATE SET
                        description = EXCLUDED.description
                """), {"database_name": database_name})
                
                # Drop temporary table
                conn.execute(text(f"DROP TABLE {temp_table}"))
                conn.commit()
        except Exception as e:
            raise Exception(f"Failed to update query examples: {str(e)}")

    def get_table_details(self, database_name: str, table_name: str) -> List[Dict[str, Any]]:
        """Get table details for a specific table in a database"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT database_name, table_name, field_name, data_type, aliases, description, key_words
                    FROM table_details
                    WHERE database_name = :database_name AND table_name = :table_name
                    ORDER BY field_name
                """), {"database_name": database_name, "table_name": table_name})
                
                return [dict(row) for row in result]
        except Exception as e:
            raise Exception(f"Failed to get table details: {str(e)}")

    def update_table_details(self, df: pd.DataFrame, database_name: str, table_name: str) -> None:
        """Update table details information in PostgreSQL"""
        try:
            # Create temporary table for the new data
            temp_table = f"temp_table_details_{database_name}_{table_name}"
            df.to_sql(temp_table, self.engine, if_exists='replace', index=False)
            
            # Merge data into the main table
            with self.engine.connect() as conn:
                conn.execute(text(f"""
                    INSERT INTO table_details (database_name, table_name, field_name, data_type, aliases, description, key_words)
                    SELECT :database_name as database_name, :table_name as table_name, 
                           field_name, data_type, aliases, description, key_words
                    FROM {temp_table}
                    ON CONFLICT (database_name, table_name, field_name)
                    DO UPDATE SET
                        data_type = EXCLUDED.data_type,
                        aliases = EXCLUDED.aliases,
                        description = EXCLUDED.description,
                        key_words = EXCLUDED.key_words
                """), {"database_name": database_name, "table_name": table_name})
                
                # Drop temporary table
                conn.execute(text(f"DROP TABLE {temp_table}"))
                conn.commit()
        except Exception as e:
            raise Exception(f"Failed to update table details: {str(e)}")
