import logging
from enum import Enum
from typing import Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class DataSource(str, Enum):
    LAKE = "lake"  # Trino + Kafka
    RDS = "rds"


class DataSourceService:
    def __init__(self):
        self._current_source: DataSource = DataSource.LAKE
        self._switch_timestamp: Optional[datetime] = None
        
    @property
    def current_source(self) -> DataSource:
        """Get current active data source"""
        return self._current_source
    
    def is_lake_active(self) -> bool:
        """Check if Lake (Trino + Kafka) is the active source"""
        return self._current_source == DataSource.LAKE
    
    def is_rds_active(self) -> bool:
        """Check if RDS is the active source"""
        return self._current_source == DataSource.RDS
    
    def switch_to_rds(self) -> dict:
        """Switch data source to RDS"""
        if self._current_source == DataSource.RDS:
            logger.warning("Already using RDS as data source")
            return {
                "status": "already_active",
                "current_source": self._current_source.value,
                "message": "RDS is already the active data source"
            }
        
        self._current_source = DataSource.RDS
        self._switch_timestamp = datetime.utcnow()
        logger.info(f"Switched data source to RDS at {self._switch_timestamp}")
        
        return {
            "status": "switched",
            "previous_source": DataSource.LAKE.value,
            "current_source": self._current_source.value,
            "switched_at": self._switch_timestamp.isoformat(),
            "message": "Successfully switched to RDS data source"
        }
    
    def switch_to_lake(self) -> dict:
        """Switch data source to Lake (Trino + Kafka)"""
        if self._current_source == DataSource.LAKE:
            logger.warning("Already using Lake as data source")
            return {
                "status": "already_active",
                "current_source": self._current_source.value,
                "message": "Lake (Trino + Kafka) is already the active data source"
            }
        
        self._current_source = DataSource.LAKE
        self._switch_timestamp = datetime.utcnow()
        logger.info(f"Switched data source to Lake at {self._switch_timestamp}")
        
        return {
            "status": "switched",
            "previous_source": DataSource.RDS.value,
            "current_source": self._current_source.value,
            "switched_at": self._switch_timestamp.isoformat(),
            "message": "Successfully switched to Lake (Trino + Kafka) data source"
        }
    
    def get_status(self) -> dict:
        """Get current data source status"""
        return {
            "current_source": self._current_source.value,
            "last_switched_at": self._switch_timestamp.isoformat() if self._switch_timestamp else None,
            "lake_active": self.is_lake_active(),
            "rds_active": self.is_rds_active()
        }


# Singleton instance
datasource_service = DataSourceService()