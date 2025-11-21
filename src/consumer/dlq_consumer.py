#!/usr/bin/env python3
"""
DLQ Consumer
Monitors and displays messages from the Dead Letter Queue
"""

import json
import logging
from confluent_kafka import Consumer, KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DLQConsumer:
    """Consumer for monitoring Dead Letter Queue messages"""
    
    def __init__(
        self,
        bootstrap_servers='localhost:9092',
        group_id='dlq-monitor-group'
    ):
        """
        Initialize DLQ consumer
        
        Args:
            bootstrap_servers: Kafka broker address
            group_id: Consumer group ID
        """
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        
        # Consumer configuration
        consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
        }
        
        self.consumer = Consumer(consumer_config)
        
        # Statistics
        self.stats = {
            'total_dlq_messages': 0,
            'errors_by_type': {},
            'errors_by_product': {},
        }
        
        logger.info(f"DLQ Consumer initialized - Group: {group_id}")
    
    def process_dlq_message(self, message_value: dict):
        """
        Process and log a DLQ message
        
        Args:
            message_value: Parsed DLQ message
        """
        self.stats['total_dlq_messages'] += 1
        
        # Extract information
        order_id = message_value.get('original_value', {}).get('orderId', 'UNKNOWN')
        product = message_value.get('original_value', {}).get('product', 'UNKNOWN')
        error_type = message_value.get('error_type', 'UNKNOWN')
        error_message = message_value.get('error_message', 'No error message')
        retry_count = message_value.get('retry_count', 0)
        
        # Update statistics
        self.stats['errors_by_type'][error_type] = \
            self.stats['errors_by_type'].get(error_type, 0) + 1
        self.stats['errors_by_product'][product] = \
            self.stats['errors_by_product'].get(product, 0) + 1
        
        # Log DLQ message
        logger.warning(
            f"\n{'='*80}\n"
            f"🚨 DLQ MESSAGE #{self.stats['total_dlq_messages']}\n"
            f"{'='*80}\n"
            f"Order ID: {order_id}\n"
            f"Product: {product}\n"
            f"Error Type: {error_type}\n"
            f"Error Message: {error_message}\n"
            f"Retry Count: {retry_count}\n"
            f"Original Topic: {message_value.get('original_topic', 'N/A')}\n"
            f"Original Partition: {message_value.get('original_partition', 'N/A')}\n"
            f"Original Offset: {message_value.get('original_offset', 'N/A')}\n"
            f"{'='*80}"
        )
    
    def monitor_dlq(self, topics=['orders-dlq']):
        """
        Monitor DLQ topic and display failed messages
        
        Args:
            topics: List of DLQ topics to monitor
        """
        self.consumer.subscribe(topics)
        
        logger.info(f"🔍 Monitoring DLQ topics: {topics}")
        logger.info("Waiting for messages... (Press Ctrl+C to stop)\n")
        
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        continue
                
                # Parse and process DLQ message
                try:
                    message_value = json.loads(msg.value().decode('utf-8'))
                    self.process_dlq_message(message_value)
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse DLQ message: {e}")
                except Exception as e:
                    logger.error(f"Error processing DLQ message: {e}")
        
        except KeyboardInterrupt:
            logger.info("\n⚠️  DLQ Monitor interrupted by user")
        
        finally:
            self.print_statistics()
            self.consumer.close()
    
    def print_statistics(self):
        """Print DLQ statistics"""
        logger.info("\n" + "=" * 80)
        logger.info("📊 DLQ STATISTICS")
        logger.info("=" * 80)
        logger.info(f"Total DLQ Messages: {self.stats['total_dlq_messages']}")
        
        if self.stats['errors_by_type']:
            logger.info("\nErrors by Type:")
            for error_type, count in sorted(
                self.stats['errors_by_type'].items(),
                key=lambda x: x[1],
                reverse=True
            ):
                logger.info(f"  {error_type}: {count}")
        
        if self.stats['errors_by_product']:
            logger.info("\nErrors by Product:")
            for product, count in sorted(
                self.stats['errors_by_product'].items(),
                key=lambda x: x[1],
                reverse=True
            ):
                logger.info(f"  {product}: {count}")
        
        logger.info("=" * 80 + "\n")


def main():
    """Main execution function"""
    try:
        # Initialize DLQ consumer
        dlq_consumer = DLQConsumer()
        
        # Start monitoring
        dlq_consumer.monitor_dlq(topics=['orders-dlq'])
        
    except Exception as e:
        logger.error(f"DLQ Consumer error: {e}", exc_info=True)


if __name__ == "__main__":
    main()
