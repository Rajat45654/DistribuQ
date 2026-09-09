from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    DATABASE_URL: str = "postgresql://distribuq:password@localhost:5432/distribuq"
    REDIS_URL: str = "redis://localhost:16379/0"
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    LOG_LEVEL: str = "INFO"
    DEFAULT_QUEUE: str = "queue:default"
    DEFAULT_PROCESSING_QUEUE: str = "queue:processing"
    DEFAULT_VISIBILITY_TIMEOUT_SEC: int = 60
    HEARTBEAT_INTERVAL_SEC: int = 3
    WORKER_TIMEOUT_SEC: int = 10


settings = Settings()
