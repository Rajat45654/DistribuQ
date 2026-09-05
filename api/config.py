from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    DATABASE_URL: str = "postgresql://distribuq:password@localhost:5432/distribuq"
    REDIS_URL: str = "redis://localhost:6379/0"
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    LOG_LEVEL: str = "INFO"
    DEFAULT_QUEUE: str = "queue:default"
    DEFAULT_VISIBILITY_TIMEOUT_SEC: int = 60


settings = Settings()
