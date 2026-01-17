from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # Trino Configuration
    TRINO_HOST: str
    TRINO_PORT: int
    TRINO_USER: str
    TRINO_SCHEMA: str
    TRINO_HTTP_SCHEMA: str
    TRINO_CATALOG: str
    TRINO_TABLE: str

    # Kafka Configuration
    KAFKA_BROKER: str
    KAFKA_TOPIC: str
    KAFKA_GROUP_ID: str

    # Redis Configuration
    REDIS_HOST: str
    REDIS_PORT: int
    REDIS_DB: int = 0
    REDIS_PASSWORD: Optional[str] = None

    # API Configuration
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000

    # Production Database (Failover)
    SSH_HOST: str
    SSH_USERNAME: str
    SSH_PRIVATE_KEY_PATH: str
    SSH_PRIVATE_KEY_PASSWORD: str
    RDS_HOST: str
    RDS_USERNAME: str
    RDS_PASSWORD: str
    RDS_DATABASE: str
    RDS_PORT: int = 3306

    # Failover Configuration
    RETRY_ATTEMPTS: int = 2
    RETRY_DELAY_SECONDS: int = 60
    POLLING_INTERVAL_SECONDS: int = 900  # 15 minutes

    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()