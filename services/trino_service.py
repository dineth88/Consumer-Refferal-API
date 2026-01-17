import trino
import logging
from typing import List, Dict, Optional
from core.config import settings

logger = logging.getLogger(__name__)


class TrinoService:
    def __init__(self):
        self.connection: Optional[trino.dbapi.Connection] = None

    def connect(self):
        """Connect to Trino"""
        try:
            self.connection = trino.dbapi.connect(
                host=settings.TRINO_HOST,
                port=settings.TRINO_PORT,
                user=settings.TRINO_USER,
                catalog=settings.TRINO_CATALOG,
                schema=settings.TRINO_SCHEMA,
                http_scheme=settings.TRINO_HTTP_SCHEMA
            )
            logger.info("Successfully connected to Trino")
        except Exception as e:
            logger.error(f"Failed to connect to Trino: {e}")
            raise

    def disconnect(self):
        """Disconnect from Trino"""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from Trino")

    def fetch_all_users(self) -> List[Dict]:
        """Fetch all users from Trino data lake"""
        try:
            cursor = self.connection.cursor()
            query = f"""
                SELECT user_id, consumer_token, platform, device_id 
                FROM {settings.TRINO_CATALOG}.{settings.TRINO_SCHEMA}.{settings.TRINO_TABLE}
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            
            users = []
            for row in rows:
                users.append({
                    "user_id": row[0],
                    "consumer_token": row[1],
                    "platform": row[2],
                    "device_id": row[3]
                })
            
            logger.info(f"Fetched {len(users)} users from Trino")
            return users
        except Exception as e:
            logger.error(f"Error fetching users from Trino: {e}")
            return []

    def health_check(self) -> bool:
        """Check Trino health"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            return True
        except Exception:
            return False


# Singleton instance
trino_service = TrinoService()