import pymysql
import logging
from typing import List, Dict, Optional
from datetime import datetime
from sshtunnel import SSHTunnelForwarder
from core.config import settings

logger = logging.getLogger(__name__)


class RDSService:
    
    def __init__(self):
        self.tunnel: Optional[SSHTunnelForwarder] = None
        self.connection: Optional[pymysql.Connection] = None
        self.last_sync_timestamp: Optional[datetime] = None

    def connect(self):
        try:
            logger.info("Creating SSH tunnel to RDS...")
            self.tunnel = SSHTunnelForwarder(
                (settings.SSH_HOST, 22),
                ssh_username=settings.SSH_USERNAME,
                ssh_pkey=settings.SSH_PRIVATE_KEY_PATH,
                ssh_private_key_password=settings.SSH_PRIVATE_KEY_PASSWORD,
                remote_bind_address=(settings.RDS_HOST, settings.RDS_PORT),
                local_bind_address=('127.0.0.1', 0)
            )
            self.tunnel.start()
            logger.info(f"SSH tunnel established on local port: {self.tunnel.local_bind_port}")
            
            self.connection = pymysql.connect(
                host='127.0.0.1',
                port=self.tunnel.local_bind_port,
                user=settings.RDS_USERNAME,
                password=settings.RDS_PASSWORD,
                database=settings.RDS_DATABASE,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            logger.info("Successfully connected to RDS")
            
        except Exception as e:
            logger.error(f"Failed to connect to RDS: {e}")
            self.disconnect()
            raise

    def disconnect(self):
        try:
            if self.connection:
                self.connection.close()
                logger.info("Closed RDS connection")
        except Exception as e:
            logger.error(f"Error closing RDS connection: {e}")
        
        try:
            if self.tunnel:
                self.tunnel.stop()
                logger.info("Closed SSH tunnel")
        except Exception as e:
            logger.error(f"Error closing SSH tunnel: {e}")

    def get_user_by_id(self, user_id: int) -> Optional[Dict]:
        """
        Get a single user by user_id from RDS
        Returns user data dict matching UserData schema if found, None otherwise
        """
        try:
            with self.connection.cursor() as cursor:
                query = f"""
                    SELECT user_id, consumer_token, platform, device_id
                    FROM {settings.TRINO_TABLE}
                    WHERE user_id = %s
                    LIMIT 1
                """
                cursor.execute(query, (user_id,))
                row = cursor.fetchone()
                
                if not row:
                    logger.debug(f"User {user_id} not found in RDS")
                    return None
                
                user_data = {
                    'user_id': row['user_id'],
                    # 'consumer_token': row['consumer_token'] or '',
                    'platform': row['platform'] or '',
                    'device_id': row['device_id'] or ''
                }
                
                logger.debug(f"Retrieved user {user_id} from RDS")
                return user_data
                
        except Exception as e:
            logger.error(f"Error fetching user {user_id} from RDS: {e}")
            raise

    def health_check(self) -> bool:
        try:
            if not self.connection or not self.tunnel or not self.tunnel.is_active:
                return False
            
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            return True
        except Exception as e:
            logger.error(f"RDS health check failed: {e}")
            return False

    def set_last_sync_timestamp(self, timestamp: datetime):
        """Set the last sync timestamp"""
        self.last_sync_timestamp = timestamp
        logger.info(f"Updated last sync timestamp to: {timestamp}")

    def get_last_sync_timestamp(self) -> Optional[datetime]:
        """Get the last sync timestamp"""
        return self.last_sync_timestamp


# Singleton instance
rds_service = RDSService()