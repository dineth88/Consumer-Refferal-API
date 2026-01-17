import json
import logging
import threading
import time
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from typing import Optional, TYPE_CHECKING
from core.config import settings
from services.redis_service import redis_service

# Avoid circular import by using TYPE_CHECKING
# if TYPE_CHECKING:
    # from services.failover_service import FailoverService

logger = logging.getLogger(__name__)


class KafkaCoordinatorWarningHandler(logging.Handler):
    def __init__(self, kafka_service_instance):
        super().__init__()
        self.kafka_service = kafka_service_instance
        self.warning_count = 0
        self.last_warning_time = 0
        self.warning_window = 30  # 30 seconds window
        self.warning_threshold = 10  # Trigger after 10 warnings
        
    def emit(self, record):
        """Process log records"""
        try:
            if record.levelno >= logging.WARNING:
                message = record.getMessage()
                
                # Check for coordinator-related warnings
                if any(keyword in message for keyword in [
                    'NodeNotReadyError',
                    'coordinator',
                    'Heartbeat session expired',
                    'connection failed',
                    'timed out',
                    'RequestTimedOutError'
                ]):
                    current_time = time.time()
                    
                    # Reset counter if outside the time window
                    if current_time - self.last_warning_time > self.warning_window:
                        self.warning_count = 0
                    
                    self.warning_count += 1
                    self.last_warning_time = current_time
                    
                    logger.debug(f"Coordinator warning detected ({self.warning_count}/{self.warning_threshold}): {message[:100]}")
                    
                    # Trigger failover if threshold reached
                    if self.warning_count >= self.warning_threshold:
                        logger.critical(
                            f"Detected {self.warning_count} Kafka coordinator warnings in {self.warning_window}s. "
                            "Lake appears to be down. Triggering failover..."
                        )
                        self.kafka_service.consecutive_errors = self.kafka_service.max_consecutive_errors
                        self.warning_count = 0  # Reset to avoid repeated triggers
                        
        except Exception as e:
            logger.error(f"Error in Kafka warning handler: {e}")


class KafkaService:
    def __init__(self):
        self.consumer: Optional[KafkaConsumer] = None
        self.consumer_thread: Optional[threading.Thread] = None
        self.running = False
        self.consecutive_errors = 0
        self.max_consecutive_errors = 3
        self.warning_handler: Optional[KafkaCoordinatorWarningHandler] = None
        self._failover_triggered = False

    def connect(self):
        """Connect to Kafka"""
        try:
            self.consumer = KafkaConsumer(
                settings.KAFKA_TOPIC,
                bootstrap_servers=[settings.KAFKA_BROKER],
                group_id=settings.KAFKA_GROUP_ID,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                consumer_timeout_ms=5000,  # 5 second timeout
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                request_timeout_ms=40000
            )
            logger.info(f"Successfully connected to Kafka topic: {settings.KAFKA_TOPIC}")
            self.consecutive_errors = 0
            self._failover_triggered = False
            
            # Setup warning handler
            self._setup_warning_handler()
            
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    def _setup_warning_handler(self):
        """Setup custom handler to monitor Kafka warnings"""
        if not self.warning_handler:
            self.warning_handler = KafkaCoordinatorWarningHandler(self)
            self.warning_handler.setLevel(logging.WARNING)
            
            # Add handler to kafka loggers
            kafka_logger = logging.getLogger('kafka')
            kafka_logger.addHandler(self.warning_handler)
            
            logger.info("Kafka coordinator warning monitor enabled")

    def disconnect(self):
        """Disconnect from Kafka"""
        self.running = False
        
        # Remove warning handler
        if self.warning_handler:
            kafka_logger = logging.getLogger('kafka')
            kafka_logger.removeHandler(self.warning_handler)
            self.warning_handler = None
        
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("Disconnected from Kafka")
            except Exception as e:
                logger.error(f"Error disconnecting from Kafka: {e}")
                
        if self.consumer_thread and self.consumer_thread.is_alive():
            self.consumer_thread.join(timeout=5)

    def process_message(self, message):
        """Process a single Kafka message"""
        try:
            # Skip messages with None value (tombstone messages)
            if message.value is None:
                logger.debug("Skipping tombstone message")
                return
            
            payload = message.value.get('payload', {})
            after_data = payload.get('after')
            
            if after_data and 'user_id' in after_data:
                user_id = after_data.get('user_id')
                consumer_token = after_data.get('consumer_token', '')
                platform = after_data.get('platform', '')
                device_id = after_data.get('device_id', '')
                
                # Check if user already exists in Redis
                if not redis_service.user_exists(user_id):
                    redis_service.add_user_data(user_id, consumer_token, platform, device_id)
                    logger.info(f"Added new user {user_id} from Kafka to Redis")
                else:
                    logger.debug(f"User {user_id} already exists in Redis, skipping")
            
            # Reset error counter on successful processing
            self.consecutive_errors = 0
            
        except Exception as e:
            logger.error(f"Error processing Kafka message: {e}")
            self.consecutive_errors += 1

    def consume_messages(self):
        logger.info("Starting Kafka consumer loop")
        no_message_count = 0
        max_no_message_iterations = 3
        
        while self.running:
            try:
                messages_received = False
                
                for message in self.consumer:
                    if not self.running:
                        break
                    self.process_message(message)
                    messages_received = True
                    no_message_count = 0  # Reset on successful message
                
                # If no messages in this iteration
                if not messages_received:
                    no_message_count += 1
                    logger.debug(f"No messages received (iteration {no_message_count}/{max_no_message_iterations})")
                    
                    # If we haven't received messages for too long AND have coordinator warnings
                    if no_message_count >= max_no_message_iterations and self.consecutive_errors > 0:
                        logger.warning(f"No messages for {no_message_count * 5} seconds with errors")
                        self.consecutive_errors += 1
                
            except KafkaError as e:
                error_msg = str(e)
                logger.error(f"Kafka error in consumer loop: {error_msg}")
                self.consecutive_errors += 1
                
            except Exception as e:
                logger.error(f"Error in Kafka consumer loop: {e}")
                self.consecutive_errors += 1
            
            # Check if we should trigger failover
            if self.consecutive_errors >= self.max_consecutive_errors:
                logger.critical(
                    f"Kafka consumer has {self.consecutive_errors} consecutive errors. "
                )
                # self._trigger_failover()
                break
            
            # Small sleep to prevent tight loop
            if self.running:
                time.sleep(1)

    def start_consuming(self):
        """Start consuming messages in a background thread"""
        if not self.running:
            self.running = True
            self.consecutive_errors = 0
            self._failover_triggered = False
            self.consumer_thread = threading.Thread(target=self.consume_messages, daemon=True)
            self.consumer_thread.start()
            logger.info("Started Kafka consumer thread")

    def stop_consuming(self):
        """Stop consuming messages from Kafka"""
        if self.running:
            logger.info("Stopping Kafka consumer...")
            self.running = False
            
            # Wait for consumer thread to finish
            if self.consumer_thread and self.consumer_thread.is_alive():
                self.consumer_thread.join(timeout=5)
                if self.consumer_thread.is_alive():
                    logger.warning("Kafka consumer thread did not stop gracefully")
                else:
                    logger.info("Kafka consumer thread stopped successfully")
            
            logger.info("Kafka consumer stopped")
        else:
            logger.info("Kafka consumer is not running")

    def health_check(self) -> bool:
        """Check Kafka health"""
        try:
            return (
                self.consumer is not None 
                and self.running 
                and self.consecutive_errors < self.max_consecutive_errors
                and not self._failover_triggered
            )
        except Exception:
            return False


# Singleton instance
kafka_service = KafkaService()