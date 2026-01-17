import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)


class RedisConnectionConfig:
    """Redis connection configuration"""
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB = int(os.getenv("REDIS_DB", "0"))
    REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")
    
    # Full connection string
    REDIS_CONNECTION_HOST = os.getenv(
        "REDIS_CONNECTION_HOST",
        f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
    )
    
    @classmethod
    def get_redis_url(cls) -> str:
        """Get Redis connection URL with password if provided"""
        if cls.REDIS_PASSWORD:
            return f"redis://:{cls.REDIS_PASSWORD}@{cls.REDIS_HOST}:{cls.REDIS_PORT}/{cls.REDIS_DB}"
        return cls.REDIS_CONNECTION_HOST


# Print configuration on import (for debugging)
if __name__ == "__main__":
    print("=== Configuration Loaded ===")
    print(f"Redis: {RedisConnectionConfig.REDIS_CONNECTION_HOST}")
