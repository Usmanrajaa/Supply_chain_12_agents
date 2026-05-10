"""LLM provider abstraction — supports OpenAI, OpenRouter, Grok (xAI), and Groq."""
from functools import lru_cache
from typing import Literal
from langchain_openai import ChatOpenAI

from common.config.settings import get_settings

settings = get_settings()
ModelTier = Literal["fast", "reasoning"]


@lru_cache(maxsize=2)
def get_llm(tier: ModelTier = "fast", temperature: float = 0.0) -> ChatOpenAI:
    model = settings.openai_model if tier == "fast" else settings.openai_reasoning_model
    api_key = settings.openai_api_key

    # Auto-detect provider based on API key prefix
    base_url = None
    if api_key.startswith("sk-or-"):
        base_url = "https://openrouter.ai/api/v1"
    elif api_key.startswith("xai-"):
        base_url = "https://api.x.ai/v1"
    elif api_key.startswith("gsk_"):          # Groq
        base_url = "https://api.groq.com/openai/v1"
    # "sk-" is OpenAI default – no base_url needed

    return ChatOpenAI(
        api_key=api_key,
        model=model,
        temperature=temperature,
        timeout=60,
        max_retries=2,
        base_url=base_url,
    )