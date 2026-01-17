import redis
import logging
from typing import Optional, Set, Dict
from core.config import settings

logger = logging.getLogger(__name__)


class RedisService:
    def __init__(self):
        self.redis_client: Optional[redis.Redis] = None
        self.user_set_key = "user_ids"
        
    def connect(self):
        try:
            self.redis_client = redis.Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_DB,
                password=settings.REDIS_PASSWORD if settings.REDIS_PASSWORD else None,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_keepalive=True,
                health_check_interval=30
            )
            # Test connection
            self.redis_client.ping()
            logger.info("Successfully connected to Redis")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise

    def disconnect(self):
        if self.redis_client:
            self.redis_client.close()
            logger.info("Disconnected from Redis")

    def add_user_data(self, user_id: int, consumer_token: str, platform: str, device_id: str):
        try:
            user_key = f"user:{user_id}"
            user_data = {
                "consumer_token": consumer_token,
                "platform": platform,
                "device_id": device_id
            }
            
            # Use pipeline for atomic operations - faster
            pipe = self.redis_client.pipeline()
            pipe.sadd(self.user_set_key, str(user_id))
            pipe.hset(user_key, mapping=user_data)
            pipe.execute()
            
            logger.debug(f"Added user {user_id} to Redis")
            return True
        except Exception as e:
            logger.error(f"Error adding user {user_id} to Redis: {e}")
            return False

    def get_user_data(self, user_id: int) -> Optional[Dict[str, any]]:
        """
        Retrieve complete user data from Redis matching UserData schema.
        Optimized with single hash lookup (no set check needed).
        Returns user data dict if found, None otherwise
        """
        try:
            # Direct hash lookup - fastest approach
            user_key = f"user:{user_id}"
            user_data = self.redis_client.hgetall(user_key)
            
            # If hash is empty, user doesn't exist
            if not user_data:
                logger.debug(f"User {user_id} not found in Redis")
                return None
            
            # Since decode_responses=True, no need to decode bytes
            # Build response matching UserData schema
            result = {
                "user_id": user_id,
                "consumer_token": user_data.get("consumer_token"),
                "platform": user_data.get("platform"),
                "device_id": user_data.get("device_id")
            }
            
            logger.debug(f"Retrieved user {user_id} data from Redis")
            return result
            
        except Exception as e:
            logger.error(f"Error retrieving user {user_id} data from Redis: {e}")
            return None
    
    def user_exists(self, user_id: int) -> bool:
        """
        Check if user exists. Uses SISMEMBER for O(1) lookup.
        """
        try:
            return self.redis_client.sismember(self.user_set_key, str(user_id))
        except Exception as e:
            logger.error(f"Error checking user {user_id} existence: {e}")
            return False

    def get_all_user_ids(self) -> Set[str]:
        try:
            return self.redis_client.smembers(self.user_set_key)
        except Exception as e:
            logger.error(f"Error getting all user IDs: {e}")
            return set()

    def get_user_count(self) -> int:
        try:
            return self.redis_client.scard(self.user_set_key)
        except Exception as e:
            logger.error(f"Error getting user count: {e}")
            return 0

    def health_check(self) -> bool:
        try:
            return self.redis_client.ping()
        except Exception:
            return False


# Singleton instance
redis_service = RedisService()