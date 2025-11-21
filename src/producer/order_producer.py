#!/usr/bin/env python3
"""
Order Producer
Produces order messages to Kafka with Avro serialization
"""

import time
import random
import logging
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OrderProducer:
    """Kafka producer for order messages using Avro serialization"""
    
    def __init__(self, bootstrap_servers='localhost:9092', schema_registry_url='http://localhost:8081'):
        """
        Initialize the order producer
        
        Args:
            bootstrap_servers: Kafka broker address
            schema_registry_url: Schema Registry URL
        """
        self.bootstrap_servers = bootstrap_servers
        self.schema_registry_url = schema_registry_url
        
        # Load Avro schema
        self.value_schema = avro.load('schemas/order.avsc')
        
        # Producer configuration
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'schema.registry.url': schema_registry_url,
            
            # Reliability settings
            'acks': 'all',  # Wait for all replicas
            'retries': 3,  # Built-in retries
            'max.in.flight.requests.per.connection': 5,
            'enable.idempotence': True,  # Exactly-once semantics
            
            # Performance settings
            'batch.size': 16384,
            'linger.ms': 10,  # Wait 10ms for batching
            'compression.type': 'snappy',
            
            # Timeout settings
            'request.timeout.ms': 30000,
            'delivery.timeout.ms': 120000,
        }
        
        # Initialize producer
        self.producer = AvroProducer(
            self.config,
            default_value_schema=self.value_schema
        )
        
        logger.info(f"Order Producer initialized - Broker: {bootstrap_servers}")
    
    def delivery_callback(self, err, msg):
        """
        Callback for message delivery reports
        
        Args:
            err: Delivery error (if any)
            msg: Delivered message
        """
        if err:
            logger.error(f"❌ Delivery failed for order: {err}")
        else:
            order_id = msg.value()['orderId']
            logger.info(
                f"✓ Delivered [{order_id}] to {msg.topic()} "
                f"[partition={msg.partition()}, offset={msg.offset()}]"
            )
    
    def generate_order(self, order_id):
        """
        Generate a random order message
        
        Args:
            order_id: Order identifier
            
        Returns:
            Order dictionary
        """
        products = [
            'Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones',
            'Webcam', 'USB Cable', 'External Drive', 'Mouse Pad', 'Docking Station'
        ]
        
        # Price ranges for different products
        price_ranges = {
            'Laptop': (799.99, 1999.99),
            'Mouse': (19.99, 79.99),
            'Keyboard': (39.99, 149.99),
            'Monitor': (199.99, 799.99),
            'Headphones': (49.99, 299.99),
            'Webcam': (39.99, 149.99),
            'USB Cable': (5.99, 19.99),
            'External Drive': (59.99, 249.99),
            'Mouse Pad': (9.99, 39.99),
            'Docking Station': (99.99, 299.99)
        }
        
        product = random.choice(products)
        min_price, max_price = price_ranges[product]
        price = round(random.uniform(min_price, max_price), 2)
        
        order = {
            'orderId': f"ORD-{order_id:04d}",
            'product': product,
            'price': price,
            'timestamp': int(time.time() * 1000)
        }
        
        return order
    
    def produce_order(self, order, topic='orders'):
        """
        Produce a single order message
        
        Args:
            order: Order dictionary
            topic: Target Kafka topic
        """
        try:
            self.producer.produce(
                topic=topic,
                value=order,
                callback=self.delivery_callback
            )
            self.producer.poll(0)  # Trigger delivery callbacks
            
        except Exception as e:
            logger.error(f"Failed to produce order {order['orderId']}: {e}")
    
    def produce_batch(self, count=20, interval=1.0, topic='orders'):
        """
        Produce a batch of order messages
        
        Args:
            count: Number of messages to produce
            interval: Delay between messages (seconds)
            topic: Target Kafka topic
        """
        logger.info(f"🚀 Starting to produce {count} orders to topic '{topic}'...")
        logger.info("=" * 70)
        
        for i in range(1, count + 1):
            order = self.generate_order(i)
            
            logger.info(
                f"📦 Producing Order {i}/{count}: "
                f"{order['orderId']} | {order['product']} | ${order['price']:.2f}"
            )
            
            self.produce_order(order, topic)
            
            if interval > 0 and i < count:
                time.sleep(interval)
        
        # Wait for all messages to be delivered
        logger.info("\n⏳ Flushing remaining messages...")
        self.producer.flush()
        
        logger.info("=" * 70)
        logger.info(f"✅ Successfully produced {count} orders!")
    
    def close(self):
        """Close the producer and flush remaining messages"""
        logger.info("Closing producer...")
        self.producer.flush()


def main():
    """Main execution function"""
    try:
        # Initialize producer
        producer = OrderProducer()
        
        # Produce a batch of orders
        # Adjust count and interval as needed
        producer.produce_batch(
            count=30,        # Number of orders
            interval=0.5,    # Delay between messages (seconds)
            topic='orders'
        )
        
        # Close producer
        producer.close()
        
    except KeyboardInterrupt:
        logger.info("\n⚠️  Producer interrupted by user")
    except Exception as e:
        logger.error(f"Producer error: {e}", exc_info=True)


if __name__ == "__main__":
    main()
