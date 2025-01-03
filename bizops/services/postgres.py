import pandas as pd
from pgvector.sqlalchemy import VECTOR
from sqlalchemy import create_engine, text, ARRAY, String, JSON
from typing import List, Dict, Any, Optional
from bizops.config import settings
from bizops.services.embedding import EmbeddingService
import uuid


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

            # Create sessions table with normalized schema
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id VARCHAR(255) PRIMARY KEY,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    last_accessed TIMESTAMP WITH TIME ZONE NOT NULL,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    current_database VARCHAR(255) REFERENCES database_info(database_name),
                    current_table VARCHAR(255),
                    intention_id VARCHAR(255),
                    FOREIGN KEY (current_database, current_table) 
                        REFERENCES table_info(database_name, table_name)
                )
            """))

            # Create chat_messages table for session history
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS chat_messages (
                    message_id VARCHAR(255) PRIMARY KEY,
                    session_id VARCHAR(255) REFERENCES sessions(session_id),
                    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                    message_type VARCHAR(50) NOT NULL,  -- 'user' or 'assistant'
                    content TEXT NOT NULL,
                    intention_id VARCHAR(255),
                    request_id VARCHAR(255) NOT NULL,
                    metadata JSONB  -- For any additional message-specific metadata
                )
            """))

            # Create session_variables table for dynamic context
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS session_variables (
                    session_id VARCHAR(255) REFERENCES sessions(session_id),
                    key VARCHAR(255) NOT NULL,
                    value_type VARCHAR(50) NOT NULL,  -- 'string', 'number', 'boolean', 'json'
                    string_value TEXT,
                    number_value DOUBLE PRECISION,
                    boolean_value BOOLEAN,
                    json_value JSONB,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    PRIMARY KEY (session_id, key)
                )
            """))

            # Create execution_plans table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS execution_plans (
                    plan_id VARCHAR(255) PRIMARY KEY,
                    session_id VARCHAR(255) REFERENCES sessions(session_id),
                    intention_id VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    status VARCHAR(50) NOT NULL,  -- 'pending', 'running', 'completed', 'failed'
                    error_message TEXT
                )
            """))

            # Create plan_tasks table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS plan_tasks (
                    task_id VARCHAR(255) PRIMARY KEY,
                    plan_id VARCHAR(255) REFERENCES execution_plans(plan_id),
                    task_type VARCHAR(100) NOT NULL,
                    task_order INTEGER NOT NULL,
                    status VARCHAR(50) NOT NULL,  -- 'pending', 'running', 'completed', 'failed'
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    started_at TIMESTAMP WITH TIME ZONE,
                    completed_at TIMESTAMP WITH TIME ZONE,
                    error_message TEXT,
                    result JSONB
                )
            """))

            # Create query_examples table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS query_examples (
                    database_name TEXT[],
                    query TEXT,
                    description TEXT,
                    keywords TEXT[],
                    embedding VECTOR(1536),
                    PRIMARY KEY (database_name, query)
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
            df['aliases'] = df['aliases'].apply(lambda x: [item.strip() for item in x.split(',')] if isinstance(x, str) else x)
            df['keywords'] = df['keywords'].apply(lambda x: [item.strip() for item in x.split(',')] if isinstance(x, str) else x)
            
            with self.engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
                conn.commit()
                
                # Specify the correct data types for array columns
                dtype = {
                    'aliases': ARRAY(String),
                    'keywords': ARRAY(String),
                    'embedding': VECTOR(1536)
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

    def update_table_info(self, df: pd.DataFrame, database_name: str) -> None:
        """Update table information in PostgreSQL"""
        try:
            with self.engine.connect() as conn:
                # Check if the corresponding database info is ready.
                result = conn.execute(text(f"""SELECT 1 FROM database_info WHERE database_name = '{database_name}' LIMIT 1"""))
                if result.rowcount == 0:
                    raise Exception(f"Failed to find corresponding database info for current table.")

                # Add embeddings to the DataFrame
                df = self.embedding_service.process_table_info(df[df['database_name']==database_name])

                # Convert string to list, then to PostgreSQL array format
                df['aliases'] = df['aliases'].apply(lambda x: [item.strip() for item in x.split(',')] if isinstance(x, str) else x)
                df['keywords'] = df['keywords'].apply(lambda x: [item.strip() for item in x.split(',')] if isinstance(x, str) else x)

                # Get the temp table prepared.
                temp_table = "temp_table_info"
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
                conn.commit()
                # Specify the correct data types for array columns
                dtype = {
                    'aliases': ARRAY(String),
                    'keywords': ARRAY(String),
                    'embedding': VECTOR(1536)
                }
                df.to_sql(temp_table, conn, if_exists='replace', index=False, dtype=dtype)
                
                conn.execute(text(f"""
                    INSERT INTO table_info (
                        database_name, table_name, aliases, description, ddl, keywords, embedding
                    )
                    SELECT 
                        database_name, table_name, aliases, description, ddl, keywords, embedding
                    FROM {temp_table}
                    ON CONFLICT (database_name, table_name)
                    DO UPDATE SET
                        aliases = EXCLUDED.aliases,
                        description = EXCLUDED.description,
                        ddl = EXCLUDED.ddl,
                        keywords = EXCLUDED.keywords,
                        embedding = EXCLUDED.embedding
                """))
                
                conn.execute(text(f"DROP TABLE {temp_table}"))
                conn.commit()
        except Exception as e:
            raise Exception(f"Failed to update table info: {str(e)}")

    def update_table_details(self, df: pd.DataFrame, database_name: str, table_name: str) -> None:
        """Update table details in PostgreSQL"""
        try:
            with self.engine.connect() as conn:
                # Check if the corresponding database info is ready.
                result = conn.execute(text(f"""SELECT 1 FROM database_info WHERE database_name = '{database_name}' LIMIT 1"""))
                if result.rowcount == 0:
                    raise Exception(f"Failed to find corresponding database for current table.")

                # Check if the corresponding table info is ready.
                result = conn.execute(text(f"""SELECT 1 FROM table_info WHERE database_name = '{database_name}' and table_name = '{table_name}' LIMIT 1"""))
                if result.rowcount == 0:
                    raise Exception(f"Failed to find corresponding table for current details.")

                # Add embeddings to the DataFrame
                df = self.embedding_service.process_table_details(df[(df['database_name']==database_name) & (df['table_name']==table_name)])

                # Convert string to list, then to PostgreSQL array format
                df['aliases'] = df['aliases'].apply(lambda x: [item.strip() for item in x.split(',')] if isinstance(x, str) else x)
                df['keywords'] = df['keywords'].apply(lambda x: [item.strip() for item in x.split(',')] if isinstance(x, str) else x)

                temp_table = "temp_table_details"
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
                conn.commit()
                
                # Specify the correct data types for array columns
                dtype = {
                    'aliases': ARRAY(String),
                    'keywords': ARRAY(String),
                    'embedding': VECTOR(1536)
                }
                df.to_sql(temp_table, conn, if_exists='replace', index=False, dtype=dtype)
                
                conn.execute(text(f"""
                    INSERT INTO table_details (
                        database_name, table_name, field_name, data_type, 
                        aliases, description, keywords, embedding
                    )
                    SELECT 
                        database_name, table_name, field_name, data_type,
                        aliases, description, keywords, embedding
                    FROM {temp_table}
                    ON CONFLICT (database_name, table_name, field_name)
                    DO UPDATE SET
                        data_type = EXCLUDED.data_type,
                        aliases = EXCLUDED.aliases,
                        description = EXCLUDED.description,
                        keywords = EXCLUDED.keywords,
                        embedding = EXCLUDED.embedding
                """))
                
                conn.execute(text(f"DROP TABLE {temp_table}"))
                conn.commit()
        except Exception as e:
            raise Exception(f"Failed to update table details: {str(e)}")

    def update_query_examples(self, df: pd.DataFrame, database_name: str) -> None:
        """Update query examples in PostgreSQL"""
        try:
            with self.engine.connect() as conn:
                # Check if the corresponding database info is ready.
                result = conn.execute(text(f"""SELECT 1 FROM database_info WHERE database_name='{database_name}' LIMIT 1"""))
                if result.rowcount == 0:
                    raise Exception(f"Failed to find corresponding database info for current table.")

                # Add embeddings to the DataFrame
                df = self.embedding_service.process_query_examples(df[df['database_name'].apply(lambda x: database_name in x)])

                # Convert string to list, then to PostgreSQL array format
                df['keywords'] = df['keywords'].apply(lambda x: [item.strip() for item in x.split(',')] if isinstance(x, str) else x)
                df['database_name'] = df['database_name'].apply(lambda x: [item.strip() for item in x.split(',')] if isinstance(x, str) else x)

                temp_table = "temp_query_examples"
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
                conn.commit()
                
                # Specify the correct data types for array columns
                dtype = {
                    'database_name': ARRAY(String),
                    'keywords': ARRAY(String),
                    'embedding': VECTOR(1536)
                }
                df.to_sql(temp_table, conn, if_exists='replace', index=False, dtype=dtype)
                
                conn.execute(text(f"""
                    INSERT INTO query_examples (
                        database_name, query, description, keywords, embedding
                    )
                    SELECT 
                        database_name, query, description, keywords, embedding
                    FROM {temp_table}
                    ON CONFLICT (database_name, query)
                    DO UPDATE SET
                        description = EXCLUDED.description,
                        keywords = EXCLUDED.keywords,
                        embedding = EXCLUDED.embedding
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

    def create_session(self, session_id: str, created_at: str, last_accessed: str) -> bool:
        """Create a new session in the database"""
        try:
            with self.engine.connect() as conn:
                conn.execute(
                    text("""
                        INSERT INTO sessions (
                            session_id, created_at, last_accessed, is_active
                        ) VALUES (
                            :session_id, :created_at, :last_accessed, TRUE
                        )
                    """),
                    {
                        "session_id": session_id,
                        "created_at": created_at,
                        "last_accessed": last_accessed
                    }
                )
                return True
        except Exception as e:
            print(f"Error creating session: {e}")
            return False

    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get session by ID with all related data"""
        with self.engine.connect() as conn:
            # Get session base info
            session = conn.execute(
                text("""
                    SELECT session_id, created_at, last_accessed, is_active,
                           current_database, current_table, intention_id
                    FROM sessions
                    WHERE session_id = :session_id
                """),
                {"session_id": session_id}
            ).first()
            
            if not session:
                return None

            # Get session variables
            variables = conn.execute(
                text("""
                    SELECT key, value_type, string_value, number_value, 
                           boolean_value, json_value
                    FROM session_variables
                    WHERE session_id = :session_id
                """),
                {"session_id": session_id}
            ).fetchall()

            # Get chat history
            chat_history = conn.execute(
                text("""
                    SELECT message_id, timestamp, message_type, content,
                           intention_id, request_id, metadata
                    FROM chat_messages
                    WHERE session_id = :session_id
                    ORDER BY timestamp ASC
                """),
                {"session_id": session_id}
            ).fetchall()

            # Build session context
            context = {}
            for var in variables:
                if var.value_type == 'string':
                    context[var.key] = var.string_value
                elif var.value_type == 'number':
                    context[var.key] = var.number_value
                elif var.value_type == 'boolean':
                    context[var.key] = var.boolean_value
                elif var.value_type == 'json':
                    context[var.key] = var.json_value

            return {
                "session_id": session.session_id,
                "created_at": session.created_at.isoformat(),
                "last_accessed": session.last_accessed.isoformat(),
                "is_active": session.is_active,
                "current_database": session.current_database,
                "current_table": session.current_table,
                "intention_id": session.intention_id,
                "context": context,
                "chat_history": [
                    {
                        "id": msg.message_id,
                        "timestamp": msg.timestamp.isoformat(),
                        "type": msg.message_type,
                        "content": msg.content,
                        "intention_id": msg.intention_id,
                        "request_id": msg.request_id,
                        "metadata": msg.metadata
                    } for msg in chat_history
                ]
            }

    def update_session_context(self, session_id: str, context: Dict[str, Any], 
                             last_accessed: str) -> bool:
        """Update session context by storing variables"""
        try:
            with self.engine.connect() as conn:
                # Update session last_accessed
                conn.execute(
                    text("""
                        UPDATE sessions
                        SET last_accessed = :last_accessed
                        WHERE session_id = :session_id
                    """),
                    {
                        "session_id": session_id,
                        "last_accessed": last_accessed
                    }
                )

                # Update or insert session variables
                for key, value in context.items():
                    value_type = type(value).__name__
                    data = {
                        "session_id": session_id,
                        "key": key,
                        "value_type": value_type,
                        "string_value": str(value) if value_type == 'str' else None,
                        "number_value": float(value) if value_type in ('int', 'float') else None,
                        "boolean_value": bool(value) if value_type == 'bool' else None,
                        "json_value": value if value_type in ('dict', 'list') else None,
                        "updated_at": last_accessed
                    }

                    conn.execute(
                        text("""
                            INSERT INTO session_variables (
                                session_id, key, value_type, string_value, 
                                number_value, boolean_value, json_value,
                                created_at, updated_at
                            ) VALUES (
                                :session_id, :key, :value_type, :string_value,
                                :number_value, :boolean_value, :json_value,
                                :updated_at, :updated_at
                            )
                            ON CONFLICT (session_id, key) DO UPDATE
                            SET value_type = :value_type,
                                string_value = :string_value,
                                number_value = :number_value,
                                boolean_value = :boolean_value,
                                json_value = :json_value,
                                updated_at = :updated_at
                        """),
                        data
                    )
                return True
        except Exception as e:
            print(f"Error updating session context: {e}")
            return False

    def add_chat_message(self, session_id: str, message_type: str, content: str,
                        intention_id: str, request_id: str, 
                        metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Add a chat message to the session history"""
        try:
            with self.engine.connect() as conn:
                conn.execute(
                    text("""
                        INSERT INTO chat_messages (
                            message_id, session_id, timestamp, message_type,
                            content, intention_id, request_id, metadata
                        ) VALUES (
                            :message_id, :session_id, NOW(), :message_type,
                            :content, :intention_id, :request_id, :metadata
                        )
                    """),
                    {
                        "message_id": str(uuid.uuid4()),
                        "session_id": session_id,
                        "message_type": message_type,
                        "content": content,
                        "intention_id": intention_id,
                        "request_id": request_id,
                        "metadata": metadata or {}
                    }
                )
                return True
        except Exception as e:
            print(f"Error adding chat message: {e}")
            return False

    def update_session_state(self, session_id: str, current_database: Optional[str] = None,
                           current_table: Optional[str] = None, 
                           intention_id: Optional[str] = None) -> bool:
        """Update session state (current database, table, intention)"""
        try:
            with self.engine.connect() as conn:
                updates = []
                params = {"session_id": session_id}

                if current_database is not None:
                    updates.append("current_database = :current_database")
                    params["current_database"] = current_database

                if current_table is not None:
                    updates.append("current_table = :current_table")
                    params["current_table"] = current_table

                if intention_id is not None:
                    updates.append("intention_id = :intention_id")
                    params["intention_id"] = intention_id

                if updates:
                    query = f"""
                        UPDATE sessions
                        SET {", ".join(updates)}
                        WHERE session_id = :session_id
                    """
                    conn.execute(text(query), params)
                return True
        except Exception as e:
            print(f"Error updating session state: {e}")
            return False

    def end_session(self, session_id: str) -> bool:
        """Mark a session as inactive"""
        try:
            with self.engine.connect() as conn:
                conn.execute(
                    text("""
                        UPDATE sessions
                        SET is_active = FALSE
                        WHERE session_id = :session_id
                    """),
                    {"session_id": session_id}
                )
                return True
        except Exception as e:
            print(f"Error ending session: {e}")
            return False

    def cleanup_expired_sessions(self, expiry_time: str) -> bool:
        """Mark sessions as inactive if they haven't been accessed since expiry_time"""
        try:
            with self.engine.connect() as conn:
                conn.execute(
                    text("""
                        UPDATE sessions
                        SET is_active = FALSE
                        WHERE last_accessed < :expiry_time
                        AND is_active = TRUE
                    """),
                    {"expiry_time": expiry_time}
                )
                return True
        except Exception as e:
            print(f"Error cleaning up sessions: {e}")
            return False
