import logging
from services.redis_service import redis_service
from services.trino_service import trino_service

logger = logging.getLogger(__name__)


class SyncService:
    """Service to sync data from Trino to Redis on startup"""

    @staticmethod
    def initial_sync():
        """Perform initial sync from Trino to Redis"""
        try:
            logger.info("Starting initial sync from Trino to Redis...")
            
            # Check current user count in Redis
            current_count = redis_service.get_user_count()
            logger.info(f"Current users in Redis: {current_count}")
            
            # Fetch all users from Trino
            users = trino_service.fetch_all_users()
            
            if not users:
                logger.warning("No users fetched from Trino")
                return
            
            # Add users to Redis (only new ones)
            added_count = 0
            skipped_count = 0
            
            for user in users:
                user_id = user.get('user_id')
                if not redis_service.user_exists(user_id):
                    redis_service.add_user_data(
                        user_id,
                        user.get('consumer_token', ''),
                        user.get('platform', ''),
                        user.get('device_id', '')
                    )
                    added_count += 1
                else:
                    skipped_count += 1
            
            final_count = redis_service.get_user_count()
            logger.info(
                f"Initial sync completed. Added: {added_count}, "
                f"Skipped: {skipped_count}, Total in Redis: {final_count}"
            )
            
        except Exception as e:
            logger.error(f"Error during initial sync: {e}")
            raise


sync_service = SyncService()