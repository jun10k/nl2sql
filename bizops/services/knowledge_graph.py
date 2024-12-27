from typing import List, Dict, Any, Optional
from llama_index.core import (
    KnowledgeGraphIndex,
    StorageContext,
    load_index_from_storage,
    load_indices_from_storage,
    load_graph_from_storage,
)
from llama_index.core.graph_stores import SimpleGraphStore
from bizops.pkg.models import ModelManager, EmbeddingType, LLMType
import os

class KnowledgeGraphService:
    def __init__(self):
        """Initialize the Knowledge Graph Service for retrieval operations"""
        # Initialize graph store
        self.graph_store = SimpleGraphStore()
        self.storage_context = StorageContext.from_defaults(graph_store=self.graph_store)
        
        # Initialize models from ModelManager
        model_manager = ModelManager.get_instance()
        self.embed_model = model_manager.get_embedding_model(EmbeddingType.AZURE_EMBEDDING)
        self.llm = model_manager.get_llm(LLMType.AZURE_GPT4O)
        
        # Load or create knowledge graph index
        self.index = self._load_or_create_index()

    def _load_or_create_index(self) -> KnowledgeGraphIndex:
        """Load existing index or create a new one"""
        try:
            # Try to load existing index
            if os.path.exists("storage"):
                return load_index_from_storage(
                    storage_context=self.storage_context,
                    llm=self.llm
                )
        except Exception as e:
            print(f"Failed to load index: {str(e)}")
        
        # Create new index if loading fails
        return KnowledgeGraphIndex(
            [],
            storage_context=self.storage_context,
            llm=self.llm,
            embed_model=self.embed_model
        )

    def query_knowledge_graph(self, 
                            query: str, 
                            include_raw: bool = False,
                            response_mode: str = "tree_summarize"
                            ) -> Dict[str, Any]:
        """
        Query the knowledge graph using natural language
        
        Args:
            query: Natural language query
            include_raw: Whether to include raw response nodes
            response_mode: Response mode for the query engine
                         ("tree_summarize", "compact", "simple", etc.)
        
        Returns:
            Dictionary containing query response and optionally raw nodes
        """
        try:
            # Create query engine with specified response mode
            query_engine = self.index.as_query_engine(
                response_mode=response_mode,
                verbose=True
            )
            
            # Execute query
            response = query_engine.query(query)
            
            # Prepare result
            result = {
                "response": str(response),
                "metadata": {
                    "response_mode": response_mode,
                }
            }
            
            # Include raw response nodes if requested
            if include_raw and hasattr(response, 'source_nodes'):
                result["raw_nodes"] = [
                    {
                        "text": node.node.text,
                        "metadata": node.node.metadata,
                        "score": node.score if hasattr(node, 'score') else None
                    }
                    for node in response.source_nodes
                ]
            
            return result
            
        except Exception as e:
            raise Exception(f"Failed to query knowledge graph: {str(e)}")

    def get_entity_info(self, entity_name: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve information about a specific entity in the knowledge graph
        
        Args:
            entity_name: Name of the entity to look up
            
        Returns:
            Dictionary containing entity information or None if not found
        """
        try:
            # Get entity from graph store
            entity_info = self.graph_store.get_node(entity_name)
            
            if entity_info:
                return {
                    "name": entity_name,
                    "metadata": entity_info.metadata,
                    "relationships": self.graph_store.get_node_relationships(entity_name)
                }
            return None
            
        except Exception as e:
            raise Exception(f"Failed to get entity info: {str(e)}")

    def list_entities(self, entity_type: Optional[str] = None) -> List[str]:
        """
        List all entities in the knowledge graph, optionally filtered by type
        
        Args:
            entity_type: Optional type to filter entities
            
        Returns:
            List of entity names
        """
        try:
            # Get all nodes from graph store
            all_nodes = self.graph_store.get_all_nodes()
            
            # Filter by type if specified
            if entity_type:
                return [
                    node.get_text() 
                    for node in all_nodes 
                    if node.metadata.get("type") == entity_type
                ]
            
            return [node.get_text() for node in all_nodes]
            
        except Exception as e:
            raise Exception(f"Failed to list entities: {str(e)}")
