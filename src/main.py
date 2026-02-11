import os
import sys
import logging
import time
import signal
import mysql.connector
from dotenv import load_dotenv

# Add src directory to path to enable relative imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cdc_processor import CDCProcessor
from kafka_producer import KafkaProducerWrapper

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CDCService:
    """CDC Service for monitoring MySQL and publishing to Kafka."""
    
    def __init__(self):
        self.db_config = {
            'host': os.getenv('CDC_DB_HOST', 'localhost'),
            'port': int(os.getenv('CDC_DB_PORT', '3306')),
            'user': os.getenv('CDC_DB_USER', 'root'),
            'password': os.getenv('CDC_DB_PASSWORD', 'root_password'),
            'database': os.getenv('CDC_DB_NAME', 'cdc_db')
        }
        self.table_name = os.getenv('CDC_TABLE_NAME', 'products')
        self.kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.kafka_topic = os.getenv('KAFKA_TOPIC', 'cdc_events')
        self.polling_interval = int(os.getenv('POLLING_INTERVAL', '5'))
        
        self.connection = None
        self.processor = CDCProcessor(self.table_name)
        self.kafka_producer = None
        self.running = True
        
        # Handle graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        
        logger.info("=" * 80)
        logger.info("CDC SERVICE INITIALIZED")
        logger.info("=" * 80)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info("\n" + "=" * 80)
        logger.info("SHUTDOWN SIGNAL RECEIVED - Closing connections...")
        logger.info("=" * 80)
        self.running = False
        self.close()
        sys.exit(0)
    
    def _connect_to_mysql(self):
        """Connect to MySQL database with retry logic."""
        retries = 0
        max_retries = 5
        while retries < max_retries:
            try:
                self.connection = mysql.connector.connect(**self.db_config)
                logger.info(f"✓ Connected to MySQL at {self.db_config['host']}:{self.db_config['port']}")
                return
            except mysql.connector.Error as e:
                retries += 1
                logger.error(f"✗ Failed to connect to MySQL (attempt {retries}/{max_retries}): {e}")
                if retries < max_retries:
                    logger.info(f"  Retrying in {2**retries} seconds...")
                    time.sleep(2 ** retries)
        raise Exception("Failed to connect to MySQL after max retries")
    
    def _fetch_table_data(self):
        """Fetch current state of the table."""
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(f"SELECT * FROM {self.table_name} ORDER BY id")
            results = cursor.fetchall()
            cursor.close()
            return results
        except mysql.connector.Error as e:
            logger.error(f"Error fetching table data: {e}")
            try:
                self.connection.close()
            except:
                pass
            self._connect_to_mysql()
            return []
    
    def poll_database(self):
        """Poll database for changes and publish events."""
        logger.info(f"\nStarting CDC polling (interval: {self.polling_interval}s)")
        logger.info(f"Table: {self.table_name}")
        logger.info(f"Kafka Topic: {self.kafka_topic}")
        logger.info("-" * 80)
        
        try:
            self._connect_to_mysql()
            self.kafka_producer = KafkaProducerWrapper(
                self.kafka_bootstrap_servers,
                self.kafka_topic
            )
            
            # Initial sync
            logger.info("\n[INITIAL SYNC]")
            current_state = self._fetch_table_data()
            self.processor.last_sync_state = {str(row['id']): row for row in current_state}
            logger.info(f"✓ Initial sync completed with {len(current_state)} records\n")
            
            poll_count = 0
            while self.running:
                try:
                    poll_count += 1
                    # Fetch current state
                    current_state = self._fetch_table_data()
                    
                    # Detect changes
                    events = self.processor.detect_changes(current_state, pk_column='id')
                    
                    # Publish events
                    for event in events:
                        if not self.kafka_producer.publish_event(event):
                            logger.error(f"✗ Failed to publish event: {event['event_id']}")
                    
                    if events:
                        logger.info(f"[POLL #{poll_count}] Detected and published {len(events)} events")
                    
                    time.sleep(self.polling_interval)
                
                except Exception as e:
                    logger.error(f"Error in polling loop: {e}")
                    time.sleep(self.polling_interval)
        
        except Exception as e:
            logger.error(f"Fatal error in CDC service: {e}")
            raise
    
    def close(self):
        """Close all connections."""
        if self.connection:
            try:
                self.connection.close()
                logger.info("✓ MySQL connection closed")
            except Exception as e:
                logger.error(f"Error closing MySQL connection: {e}")
        
        if self.kafka_producer:
            self.kafka_producer.close()

def main():
    logger.info("\n")
    logger.info("╔" + "═" * 78 + "╗")
    logger.info("║" + " " * 20 + "EVENT-DRIVEN CDC SERVICE" + " " * 35 + "║")
    logger.info("╚" + "═" * 78 + "╝")
    
    service = CDCService()
    try:
        service.poll_database()
    except KeyboardInterrupt:
        logger.info("Service interrupted by user")
        service.close()
    except Exception as e:
        logger.error(f"Service failed: {e}")
        service.close()
        sys.exit(1)

if __name__ == "__main__":
    main()
