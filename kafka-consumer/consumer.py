import os
import logging
import json
import signal
import sys
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
topic = os.getenv('KAFKA_TOPIC', 'cdc_events')

logger.info("Starting Kafka Consumer...")
logger.info(f"Bootstrap servers: {bootstrap_servers}")
logger.info(f"Topic: {topic}\n")

try:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='cdc-consumer-group',
        enable_auto_commit=True
    )
    
    logger.info(f"Connected! Listening for CDC events on '{topic}'...\n")
    
    event_count = 0
    for message in consumer:
        event_count += 1
        event = message.value
        logger.info("=" * 80)
        logger.info(f"[EVENT #{event_count}] CDC Event Received")
        logger.info("=" * 80)
        logger.info(f"Event ID:       {event.get('event_id')}")
        logger.info(f"Timestamp:      {event.get('timestamp')}")
        logger.info(f"Table:          {event.get('table_name')}")
        logger.info(f"Operation:      {event.get('operation_type')}")
        logger.info(f"Primary Keys:   {json.dumps(event.get('primary_keys'), indent=16)}")
        
        payload = event.get('payload', {})
        if payload.get('old_data'):
            logger.info(f"Old Data:       {json.dumps(payload.get('old_data'), indent=16)}")
        if payload.get('new_data'):
            logger.info(f"New Data:       {json.dumps(payload.get('new_data'), indent=16)}")
        
        logger.info("=" * 80 + "\n")

except KafkaError as e:
    logger.error(f"Kafka error: {e}")
except KeyboardInterrupt:
    logger.info("Consumer stopped")
finally:
    consumer.close()
