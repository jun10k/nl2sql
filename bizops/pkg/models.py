import os
from enum import Enum
from typing import Dict, Any

from llama_index.embeddings.dashscope import (
    DashScopeEmbedding,
    DashScopeTextEmbeddingModels,
    DashScopeTextEmbeddingType,
)
from llama_index.llms.anthropic import Anthropic
from llama_index.llms.dashscope import DashScope
from llama_index.llms.openai import OpenAI
from llama_index.llms.azure_openai import AzureOpenAI
from llama_index.embeddings.azure_openai import AzureOpenAIEmbedding


class LLMType(Enum):
    GPT4O = "gpt-4o"
    CLAUDE = "claude-2"
    QWEN = "qwen-plus"
    AZURE_GPT4O = "gpt-4o"

class EmbeddingType(Enum):
    QWEN_DOCUMENT = DashScopeTextEmbeddingModels.TEXT_EMBEDDING_V3
    QWEN_QUERY = DashScopeTextEmbeddingModels.TEXT_EMBEDDING_V3
    QWEN_DIMENSION = 1024
    AZURE_EMBEDDING = "text-embedding-3-large"
    AZURE_DIMENSION = 1536

class ModelManager:
    _instance = None
    _llm_cache: Dict[str, Any] = {}
    _embedding_cache: Dict[str, Any] = {}
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ModelManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self.openai_api_key = os.getenv("OPENAI_API_KEY")
            self.anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")
            self.dashscope_api_key = os.getenv("DASHSCOPE_API_KEY")
            # Azure OpenAI credentials
            self.azure_api_key = os.getenv("AZURE_OPENAI_API_KEY")
            self.azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
            self._initialized = True

    @classmethod
    def get_instance(cls) -> 'ModelManager':
        """Get the singleton instance of ModelManager"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def get_llm(self, model_type: LLMType, **kwargs):
        """Get LLM model instance by type."""
        if model_type.value in self._llm_cache:
            return self._llm_cache[model_type.value]

        if model_type in [LLMType.GPT4O]:
            model = OpenAI(
                api_key=self.openai_api_key,
                model=model_type.value,
                temperature=kwargs.get('temperature', 0),
                **kwargs
            )
        elif model_type == LLMType.AZURE_GPT4O:
            model = OpenAI(
                model=model_type.value,
                temperature=kwargs.get('temperature', 0),
                api_key=self.azure_api_key,
                api_base=self.azure_endpoint,
                api_type="azure",
                api_version="2024-10-21",
                deployment_name=LLMType.AZURE_GPT4O.value,
                **kwargs
            )
        elif model_type == LLMType.CLAUDE:
            model = Anthropic(
                api_key=self.anthropic_api_key,
                model=model_type.value,
                temperature=kwargs.get('temperature', 0),
                **kwargs
            )
        elif model_type == LLMType.QWEN:
            model = DashScope(
                api_key=self.dashscope_api_key,
                model=model_type.value,
                temperature=kwargs.get('temperature', 0),
                **kwargs
            )
        else:
            raise ValueError(f"Unsupported LLM type: {model_type}")

        self._llm_cache[model_type.value] = model
        return model

    def get_embedding_model(self, model_type: EmbeddingType, **kwargs):
        """Get embedding model instance by type."""
        if model_type.value in self._embedding_cache:
            return self._embedding_cache[model_type.value]

        if model_type == EmbeddingType.AZURE_EMBEDDING:
            model = AzureOpenAIEmbedding(
                model=model_type.value,
                deployment_name=model_type.value,
                api_key=self.azure_api_key,
                azure_endpoint=self.azure_endpoint,
                api_version="2024-10-21",
                dimensions=EmbeddingType.AZURE_DIMENSION.value,
                **kwargs
            )
        elif model_type == EmbeddingType.QWEN_DOCUMENT:
            model = DashScopeEmbedding(model_name=model_type.value, 
                                       text_type=DashScopeTextEmbeddingType.TEXT_TYPE_DOCUMENT,)
        elif model_type == EmbeddingType.QWEN_QUERY:
            model = DashScopeEmbedding(model_name=model_type.value, 
                                       text_type=DashScopeTextEmbeddingType.TEXT_TYPE_QUERY,)
        else:
            raise ValueError(f"Unsupported embedding type: {model_type}")

        self._embedding_cache[model_type.value] = model
        return model

    def clear_cache(self):
        """Clear model caches."""
        self._llm_cache.clear()
        self._embedding_cache.clear()