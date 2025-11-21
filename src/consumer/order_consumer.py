#!/usr/bin/env python3
"""
Order Consumer
Consumes order messages from Kafka with JSON deserialization,
performs real-time price aggregation, and handles failures with retry logic
"""

import sys
import os
import json
import logging
from confluent_kafka import Consumer, Producer, KafkaError

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from aggregator.price_aggregator import PriceAggregator
from retry.retry_handler import RetryHandler, RetryableError, PermanentError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OrderConsumer:
    """Kafka consumer for order messages with aggregation and error handling"""
    
    def __init__(
        self,
        bootstrap_servers='localhost:9092',
        schema_registry_url='http://localhost:8081',
        group_id='order-processor-group'
    ):
        """
        Initialize the order consumer
        
        Args:
            bootstrap_servers: Kafka broker address
            schema_registry_url: Schema Registry URL
            group_id: Consumer group ID
        """
        self.bootstrap_servers = bootstrap_servers
        self.schema_registry_url = schema_registry_url
        self.group_id = group_id
        
        # Consumer configuration
        consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            
            # Offset management
            'auto.offset.reset': 'earliest',  # Start from beginning if no offset
            'enable.auto.commit': False,  # Manual commit for reliability
            
            # Consumer behavior
            'max.poll.interval.ms': 300000,  # 5 minutes max processing time
            'session.timeout.ms': 45000,
            'heartbeat.interval.ms': 3000,
        }
        
        # Initialize consumer
        self.consumer = Consumer(consumer_config)
        
        # Initialize DLQ producer
        dlq_config = {
            'bootstrap.servers': bootstrap_servers,
            'compression.type': 'snappy',
        }
        self.dlq_producer = Producer(dlq_config)
        
        # Initialize components
        self.aggregator = PriceAggregator()
        self.retry_handler = RetryHandler(
            max_retries=3,
            initial_delay=1.0,
            backoff_multiplier=2.0,
            max_delay=10.0
        )
        
        # Statistics
        self.stats = {
            'processed': 0,
            'failed': 0,
            'retried': 0,
            'sent_to_dlq': 0
        }
        
        logger.info(f"Order Consumer initialized - Group: {group_id}")
    
    def process_order(self, order: dict) -> None:
        """
        Process a single order message
        
        Args:
            order: Order dictionary from Kafka
            
        Raises:
            RetryableError: For temporary failures
            PermanentError: For permanent failures
        """
        # Validate order data
        if not all(key in order for key in ['orderId', 'product', 'price']):
            raise PermanentError(f"Invalid order format: missing required fields")
        
        if order['price'] <= 0:
            raise PermanentError(f"Invalid price: {order['price']}")
        
        # Simulate occasional processing failure for demo (5% chance)
        # Comment out in production
        import random
        if random.random() < 0.05:
            raise RetryableError("Simulated temporary processing failure")
        
        # Update aggregation
        avg = self.aggregator.update(order['product'], order['price'])
        
        # Log processed order
        logger.info(
            f"✓ Processed [{order['orderId']}] {order['product']} @ ${order['price']:.2f} "
            f"| Running Avg: ${avg:.2f}"
        )
    
    def send_to_dlq(self, message, error: Exception, retry_count: int = 0):
        """
        Send failed message to Dead Letter Queue
        
        Args:
            message: Original Kafka message
            error: Exception that caused the failure
            retry_count: Number of retry attempts made
        """
        try:
            # Create DLQ message with metadata
            dlq_message = {
                'original_topic': message.topic(),
                'original_partition': message.partition(),
                'original_offset': message.offset(),
                'original_key': message.key().decode('utf-8') if message.key() else None,
                'original_value': message.value(),
                'error_message': str(error),
                'error_type': type(error).__name__,
                'retry_count': retry_count,
                'failed_at': message.timestamp()[1] if message.timestamp()[0] else None,
                'consumer_group': self.group_id
            }
            
            # Produce to DLQ
            self.dlq_producer.produce(
                topic='orders-dlq',
                key=message.key(),
                value=json.dumps(dlq_message).encode('utf-8')
            )
            self.dlq_producer.flush()
            
            order_id = message.value().get('orderId', 'UNKNOWN')
            logger.warning(f"📮 Sent to DLQ: {order_id} - {error}")
            
            self.stats['sent_to_dlq'] += 1
            
        except Exception as e:
            logger.error(f"Failed to send message to DLQ: {e}")
    
    def consume_messages(self, topics=['orders'], max_messages=None):
        """
        Consume and process messages from Kafka topics
        
        Args:
            topics: List of topics to subscribe to
            max_messages: Maximum number of messages to process (None for infinite)
        """
        self.consumer.subscribe(topics)
        
        logger.info(f"🚀 Starting consumer for topics: {topics}")
        logger.info("=" * 80)
        
        message_count = 0
        
        try:
            while True:
                # Check if max messages reached
                if max_messages and message_count >= max_messages:
                    logger.info(f"Reached max messages limit: {max_messages}")
                    break
                
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition - not an error
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        continue
                
                # Deserialize JSON message
                order = json.loads(msg.value().decode('utf-8'))
                
                # Process message with retry logic
                try:
                    self.retry_handler.execute_with_retry(
                        self.process_order,
                        order,
                        error_context=f"Order {order.get('orderId', 'UNKNOWN')}"
                    )
                    
                    # Commit offset after successful processing
                    self.consumer.commit(message=msg)
                    self.stats['processed'] += 1
                    message_count += 1
                    
                except PermanentError as e:
                    # Send to DLQ and commit to move forward
                    self.send_to_dlq(msg, e, retry_count=self.retry_handler.max_retries)
                    self.consumer.commit(message=msg)
                    self.stats['failed'] += 1
                    message_count += 1
                    
                except Exception as e:
                    logger.error(f"Unexpected error processing message: {e}")
                    self.send_to_dlq(msg, e, retry_count=0)
                    self.consumer.commit(message=msg)
                    self.stats['failed'] += 1
                    message_count += 1
        
        except KeyboardInterrupt:
            logger.info("\n⚠️  Consumer interrupted by user")
        
        finally:
            self.close()
    
    def print_statistics(self):
        """Print consumer statistics"""
        logger.info("\n" + "=" * 80)
        logger.info("📈 CONSUMER STATISTICS")
        logger.info("=" * 80)
        logger.info(f"Messages Processed: {self.stats['processed']}")
        logger.info(f"Messages Failed: {self.stats['failed']}")
        logger.info(f"Sent to DLQ: {self.stats['sent_to_dlq']}")
        logger.info("=" * 80 + "\n")
        
        # Print aggregation summary
        self.aggregator.print_summary()
    
    def close(self):
        """Close consumer and producer connections"""
        logger.info("\nClosing consumer...")
        
        # Print final statistics
        self.print_statistics()
        
        # Close connections
        self.consumer.close()
        self.dlq_producer.flush()
        
        logger.info("Consumer closed")


def main():
    """Main execution function"""
    try:
        # Initialize consumer
        consumer = OrderConsumer()
        
        # Start consuming messages
        # Set max_messages=None for continuous consumption
        consumer.consume_messages(
            topics=['orders'],
            max_messages=None  # Process all messages
        )
        
    except Exception as e:
        logger.error(f"Consumer error: {e}", exc_info=True)


if __name__ == "__main__":
    main()
