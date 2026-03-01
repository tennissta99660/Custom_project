from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class Settings(BaseSettings):
    # App
    PROJECT_NAME: str = "Atlas Search Engine"
    DEBUG: bool = True

    # PostgreSQL
    POSTGRES_URL: str

    # Neo4j
    NEO4J_URI: str
    NEO4J_USER: str
    NEO4J_PASSWORD: str
    NEO4J_DB: str = "atlas"

    # Redis
    REDIS_URL: str

    # ClickHouse
    CLICKHOUSE_HOST: str = "localhost"
    CLICKHOUSE_PORT: int = 8123
    CLICKHOUSE_USER: str = "default"
    CLICKHOUSE_PASSWORD: str = ""

    # EDGAR (SEC Filings)
    EDGAR_USER_AGENT: str = "YourName yourname@email.com"

    # APIs
    FYERS_APP_ID: str = ""
    FYERS_SECRET: str = ""

    # News APIs
    FINNHUB_API_KEY: str = ""
    NEWSDATA_API_KEY: str = ""
    GDELT_UPDATE_INTERVAL: int = 900  # seconds (15 min)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

settings = Settings()
