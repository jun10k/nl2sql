from typing import Optional, Dict, Any
from enum import Enum
from llama_index.llms import OpenAI, Anthropic, ModelScopeLLM
from llama_index.embeddings import OpenAIEmbedding, ModelScopeEmbedding
import os

class LLMType(Enum):
    GPT4O = "gpt-4-0613"
    GPT4O_TURBO = "gpt-4-0125-preview"
    CLAUDE = "claude-2"
    QWEN = "qwen-max"

class EmbeddingType(Enum):
    OPENAI = "text-embedding-ada-002"
    QWEN = "text2vec-base-chinese"

class ModelManager:
    _instance = None
    _llm_cache: Dict[str, Any] = {}
    _embedding_cache: Dict[str, Any] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ModelManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        self.anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")
        self.modelscope_api_key = os.getenv("MODELSCOPE_API_KEY")

    def get_llm(self, model_type: LLMType, **kwargs):
        """Get LLM model instance by type."""
        if model_type.value in self._llm_cache:
            return self._llm_cache[model_type.value]

        if model_type in [LLMType.GPT4O, LLMType.GPT4O_TURBO]:
            model = OpenAI(
                api_key=self.openai_api_key,
                model=model_type.value,
                temperature=kwargs.get('temperature', 0),
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
            model = ModelScopeLLM(
                api_key=self.modelscope_api_key,
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

        if model_type == EmbeddingType.OPENAI:
            model = OpenAIEmbedding(
                api_key=self.openai_api_key,
                model=model_type.value,
                **kwargs
            )
        elif model_type == EmbeddingType.QWEN:
            model = ModelScopeEmbedding(
                api_key=self.modelscope_api_key,
                model=model_type.value,
                **kwargs
            )
        else:
            raise ValueError(f"Unsupported embedding type: {model_type}")

        self._embedding_cache[model_type.value] = model
        return model

    def clear_cache(self):
        """Clear model caches."""
        self._llm_cache.clear()
        self._embedding_cache.clear()