import json
import logging
from typing import Dict, Any
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time

logger = logging.getLogger(__name__)

class KafkaProducerWrapper:
    """Kafka producer with retry logic and error handling."""
    
    def __init__(self, bootstrap_servers: str, topic: str, max_retries: int = 3):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.max_retries = max_retries
        self.producer = None
        self._connect()
    
    def _connect(self):
        """Connect to Kafka broker with retry logic."""
        retries = 0
        while retries < self.max_retries:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',
                    retries=3,
                    max_in_flight_requests_per_connection=1,
                    request_timeout_ms=30000
                )
                logger.info(f"✓ Connected to Kafka at {self.bootstrap_servers}")
                return
            except Exception as e:
                retries += 1
                logger.error(f"✗ Failed to connect to Kafka (attempt {retries}/{self.max_retries}): {e}")
                if retries < self.max_retries:
                    time.sleep(2 ** retries)  # Exponential backoff
        raise Exception("Failed to connect to Kafka after max retries")
    
    def publish_event(self, event: Dict[str, Any]) -> bool:
        """Publish an event to Kafka with retry logic."""
        retries = 0
        while retries < self.max_retries:
            try:
                future = self.producer.send(self.topic, value=event)
                record_metadata = future.get(timeout=10)
                logger.info(f"✓ Event {event['event_id'][:8]}... published to partition {record_metadata.partition} offset {record_metadata.offset}")
                return True
            except KafkaError as e:
                retries += 1
                logger.error(f"✗ Failed to publish event (attempt {retries}/{self.max_retries}): {e}")
                if retries < self.max_retries:
                    time.sleep(2 ** retries)
        logger.error(f"✗ Failed to publish event after {self.max_retries} retries")
        return False
    
    def close(self):
        """Close Kafka producer connection gracefully."""
        if self.producer:
            try:
                self.producer.flush()
                self.producer.close()
                logger.info("✓ Kafka producer closed")
            except Exception as e:
                logger.error(f"Error closing producer: {e}")
