from typing import Dict, Any, List, Optional
from uuid_extensions import uuid7, uuid7str
import time
from enum import Enum
from bizops.services.vector import VectorService
from bizops.services.embedding import EmbeddingService
from bizops.services.knowledge_graph import KnowledgeGraphService

class MetadataType(Enum):
    DATABASE_METADATA = "database_metadata"
    TABLE_METADATA = "table_metadata"
    QUERY_EXAMPLE = "query_example"
    KNOWLEDGE_GRAPH = "knowledge_graph"
    TASK_RELATIONSHIP = "task_relationship"

class NodeType(Enum):
    TABLE = "table"
    COLUMN = "column"
    FOREIGN_KEY = "foreign_key"
    PRIMARY_KEY = "primary_key"
    INDEX = "index"
    TASK = "task"
    INTENTION = "intention"

class EdgeType(Enum):
    HAS_COLUMN = "has_column"
    REFERENCES = "references"
    INDEXED_BY = "indexed_by"
    RELATED_TO = "related_to"
    JOINED_WITH = "joined_with"
    DEPENDS_ON = "depends_on"
    SIMILAR_TO = "similar_to"
    USED_IN = "used_in"

class ContextRelevance(Enum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    CRITICAL = 4

class ContextAgent:
    def __init__(self):
        self.vector_service = VectorService()
        self.embedding_service = EmbeddingService()
        self.knowledge_graph = KnowledgeGraphService()

    def search_database_metadata(self, intention: Dict[str, Any], database_name: str) -> Dict[str, Any]:
        """
        Search for database metadata based on intention
        Returns information about databases, schemas, tables, and their descriptions
        """
        try:
            # Extract search query from intention
            query = self._build_search_query(intention)
            
            # Search for database-level metadata
            db_results = self.vector_service.search_similar_documents(
                query=query,
                filter_metadata={
                    "type": MetadataType.DATABASE_METADATA.value,
                    "database_name": database_name
                },
                limit=5
            )

            # Get database description and configuration
            db_info = self.knowledge_graph.get_database_info(database_name)
            
            # Get schema information
            schema_info = self.knowledge_graph.get_schema_info(database_name)
            
            return {
                "type": MetadataType.DATABASE_METADATA.value,
                "results": {
                    "database": {
                        "name": database_name,
                        "info": db_info,
                        "similar_contexts": db_results
                    },
                    "schemas": schema_info
                },
                "metadata": {
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S+08:00"),
                    "search_id": str(uuid7()),
                    "intention_id": intention["id"]
                }
            }
        except Exception as e:
            raise Exception(f"Error searching database metadata: {str(e)}")

    def search_table_metadata(self, intention: Dict[str, Any], database_name: str) -> Dict[str, Any]:
        """
        Search for relevant table metadata based on intention
        """
        try:
            # Extract search query from intention
            query = self._build_search_query(intention)
            
            # Search for table metadata
            results = self.vector_service.search_similar_documents(
                query=query,
                filter_metadata={
                    "type": MetadataType.TABLE_METADATA.value,
                    "database_name": database_name
                },
                limit=5
            )
            
            return {
                "type": MetadataType.TABLE_METADATA.value,
                "results": results,
                "metadata": {
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S+08:00"),
                    "search_id": str(uuid7()),
                    "intention_id": intention["id"]
                }
            }
        except Exception as e:
            raise Exception(f"Error searching table metadata: {str(e)}")

    def search_query_examples(self, intention: Dict[str, Any], database_name: str) -> Dict[str, Any]:
        """
        Search for relevant query examples based on intention
        """
        try:
            # Extract search query from intention
            query = self._build_search_query(intention)
            
            # Search for query examples
            results = self.vector_service.search_similar_documents(
                query=query,
                filter_metadata={
                    "type": MetadataType.QUERY_EXAMPLE.value,
                    "database_name": database_name
                },
                limit=3
            )
            
            return {
                "type": MetadataType.QUERY_EXAMPLE.value,
                "results": results,
                "metadata": {
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S+08:00"),
                    "search_id": str(uuid7()),
                    "intention_id": intention["id"]
                }
            }
        except Exception as e:
            raise Exception(f"Error searching query examples: {str(e)}")

    def search_database_schema_relationships(self, intention: Dict[str, Any], database_name: str) -> Dict[str, Any]:
        """
        Search knowledge graph for relationships from tasks to database objects (tables, fields, etc.)
        Helps understand how database objects are used in different tasks
        """
        try:
            entities = intention["analysis"].get("entities", {})
            tables = entities.get("tables", [])
            fields = entities.get("fields", [])
            
            # Build natural language query for schema relationships
            query_parts = []
            if tables:
                query_parts.append(f"Find relationships for tables: {', '.join(tables)}")
            if fields:
                query_parts.append(f"Find relationships for fields: {', '.join(fields)}")
            if database_name:
                query_parts.append(f"in database {database_name}")
            
            query = " ".join(query_parts)
            
            # Query knowledge graph
            kg_response = self.knowledge_graph.query_knowledge_graph(
                query=query,
                include_raw=True,
                response_mode="compact"
            )
            
            # Get specific entity information for each table and field
            relationships = []
            join_paths = []
            
            # Get table relationships
            for table in tables:
                table_info = self.knowledge_graph.get_entity_info(f"{database_name}.{table}")
                if table_info:
                    relationships.extend(table_info["relationships"])
            
            # Get field relationships
            for field in fields:
                if "." in field:
                    field_info = self.knowledge_graph.get_entity_info(f"{database_name}.{field}")
                    if field_info:
                        relationships.extend(field_info["relationships"])
            
            # Extract join paths from relationships
            for rel in relationships:
                if rel.get("type") == EdgeType.JOINED_WITH.value:
                    join_paths.append(rel)
            
            return {
                "type": MetadataType.KNOWLEDGE_GRAPH.value,
                "results": {
                    "relationships": relationships,
                    "join_paths": join_paths,
                    "kg_response": kg_response["response"],
                    "raw_nodes": kg_response.get("raw_nodes", [])
                },
                "metadata": {
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S+08:00"),
                    "search_id": str(uuid7()),
                    "intention_id": intention["id"]
                }
            }
        except Exception as e:
            raise Exception(f"Error searching database schema relationships: {str(e)}")

    def search_intention_task_relationships(self, intention: Dict[str, Any]) -> Dict[str, Any]:
        """
        Search knowledge graph for relationships from current intention to other intentions and tasks
        Helps understand how the current intention relates to existing patterns and solutions
        """
        try:
            # Extract search query from intention
            query = self._build_search_query(intention)
            intent_type = intention["analysis"]["primary_intent"]
            
            # Query knowledge graph for similar intentions and tasks
            kg_response = self.knowledge_graph.query_knowledge_graph(
                query=query,
                include_raw=True,
                response_mode="compact"
            )
            
            # Get similar intentions
            similar_intentions = []
            intention_patterns = []
            for node in kg_response.get("raw_nodes", []):
                if node["metadata"].get("type") == NodeType.INTENTION.value:
                    similar_intentions.append(node)
                    # Get intention info including patterns
                    intention_info = self.knowledge_graph.get_entity_info(node["text"])
                    if intention_info and "patterns" in intention_info["metadata"]:
                        intention_patterns.extend(intention_info["metadata"]["patterns"])
            
            # Get similar tasks based on patterns
            similar_tasks = []
            for node in kg_response.get("raw_nodes", []):
                if (node["metadata"].get("type") == NodeType.TASK.value and
                    any(pattern in node["metadata"].get("patterns", []) 
                        for pattern in intention_patterns)):
                    similar_tasks.append(node)
            
            # Get relationships
            relationships = []
            
            # Get intention relationships
            for intention_node in similar_intentions:
                intention_info = self.knowledge_graph.get_entity_info(intention_node["text"])
                if intention_info:
                    relationships.extend(intention_info["relationships"])
            
            # Get task relationships
            for task_node in similar_tasks:
                task_info = self.knowledge_graph.get_entity_info(task_node["text"])
                if task_info:
                    relationships.extend(task_info["relationships"])
            
            return {
                "type": MetadataType.TASK_RELATIONSHIP.value,
                "results": {
                    "similar_intentions": similar_intentions,
                    "similar_tasks": similar_tasks,
                    "intention_patterns": intention_patterns,
                    "relationships": relationships,
                    "kg_response": kg_response["response"]
                },
                "metadata": {
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S+08:00"),
                    "search_id": str(uuid7()),
                    "intention_id": intention["id"],
                    "intent_type": intent_type
                }
            }
        except Exception as e:
            raise Exception(f"Error searching intention and task relationships: {str(e)}")

    def enrich_context(self, intention: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich intention with relevant database and task context
        """
        try:
            # Get database name from intention
            database_name = intention["analysis"].get("database_name")
            if not database_name:
                raise ValueError("Database name not found in intention")

            # Search all sources
            db_metadata = self.search_database_metadata(intention, database_name)
            table_metadata = self.search_table_metadata(intention, database_name)
            query_examples = self.search_query_examples(intention, database_name)
            schema_relationships = self.search_database_schema_relationships(intention, database_name)
            intention_relationships = self.search_intention_task_relationships(intention)

            # Combine results
            return {
                "database_context": {
                    "database_metadata": db_metadata["results"],
                    "table_metadata": table_metadata["results"],
                    "query_examples": query_examples["results"],
                    "schema_relationships": schema_relationships["results"]
                },
                "task_context": intention_relationships["results"],
                "metadata": {
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S+08:00"),
                    "context_id": str(uuid7()),
                    "intention_id": intention["id"],
                    "database_name": database_name
                }
            }
        except Exception as e:
            raise Exception(f"Error enriching context: {str(e)}")

    def _build_search_query(self, intention: Dict[str, Any]) -> str:
        """Build natural language query from intention"""
        query_parts = []
        
        # Add primary intent
        if "primary_intent" in intention["analysis"]:
            query_parts.append(f"Intent: {intention['analysis']['primary_intent']}")
        
        # Add entities
        entities = intention["analysis"].get("entities", {})
        for entity_type, values in entities.items():
            if values:
                query_parts.append(f"{entity_type}: {', '.join(values)}")
        
        # Add constraints
        constraints = intention["analysis"].get("constraints", [])
        if constraints:
            query_parts.append(f"Constraints: {', '.join(constraints)}")
        
        return " ".join(query_parts)
