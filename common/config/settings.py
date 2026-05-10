"""Centralized settings loaded from environment variables."""
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_env: str = "development"
    log_level: str = "INFO"

    openai_api_key: str = "sk-replace-me"
    openai_model: str = "gpt-4o-mini"
    openai_reasoning_model: str = "gpt-4o"

    redis_url: str = "redis://localhost:6379/0"
    event_stream_prefix: str = "supply_chain"

    database_url: str = "postgresql+asyncpg://supply_chain:changeme@localhost:5432/supply_chain"

    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "changeme12"

    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "supply-chain-docs"

    high_value_threshold: float = 10000.0
    reorder_safety_factor: float = 1.5
    vendor_delay_threshold_hours: int = 24


@lru_cache
def get_settings() -> Settings:
    return Settings()
