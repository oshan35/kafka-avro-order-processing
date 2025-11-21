#!/usr/bin/env python3
"""
Kafka Utilities
Common utilities for Kafka operations
"""

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_producer_config(bootstrap_servers='localhost:9092', schema_registry_url='http://localhost:8081'):
    """
    Get standard producer configuration
    
    Args:
        bootstrap_servers: Kafka broker address
        schema_registry_url: Schema Registry URL
        
    Returns:
        Dictionary with producer configuration
    """
    return {
        'bootstrap.servers': bootstrap_servers,
        'schema.registry.url': schema_registry_url,
        'acks': 'all',
        'retries': 3,
        'max.in.flight.requests.per.connection': 5,
        'enable.idempotence': True,
        'batch.size': 16384,
        'linger.ms': 10,
        'compression.type': 'snappy',
        'request.timeout.ms': 30000,
        'delivery.timeout.ms': 120000,
    }


def get_consumer_config(
    bootstrap_servers='localhost:9092',
    schema_registry_url='http://localhost:8081',
    group_id='default-group'
):
    """
    Get standard consumer configuration
    
    Args:
        bootstrap_servers: Kafka broker address
        schema_registry_url: Schema Registry URL
        group_id: Consumer group ID
        
    Returns:
        Dictionary with consumer configuration
    """
    return {
        'bootstrap.servers': bootstrap_servers,
        'schema.registry.url': schema_registry_url,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'max.poll.interval.ms': 300000,
        'session.timeout.ms': 45000,
        'heartbeat.interval.ms': 3000,
        'fetch.min.bytes': 1,
        'fetch.max.wait.ms': 500,
    }


def format_order_message(order):
    """
    Format order message for display
    
    Args:
        order: Order dictionary
        
    Returns:
        Formatted string
    """
    return (
        f"Order {order['orderId']}: "
        f"{order['product']} @ ${order['price']:.2f}"
    )
