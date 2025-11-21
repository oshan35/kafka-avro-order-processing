#!/usr/bin/env python3
"""
Test suite for PriceAggregator
"""

import pytest
import sys
sys.path.append('..')

from src.aggregator.price_aggregator import PriceAggregator


def test_aggregator_initialization():
    """Test aggregator initializes with empty state"""
    aggregator = PriceAggregator()
    assert aggregator.total_orders == 0
    assert aggregator.total_revenue == 0.0
    assert len(aggregator.state) == 0


def test_single_product_update():
    """Test updating a single product"""
    aggregator = PriceAggregator()
    avg = aggregator.update('Laptop', 999.99)
    
    assert avg == 999.99
    assert aggregator.total_orders == 1
    assert aggregator.total_revenue == 999.99


def test_multiple_updates_same_product():
    """Test running average calculation for same product"""
    aggregator = PriceAggregator()
    
    aggregator.update('Laptop', 900.0)
    aggregator.update('Laptop', 1000.0)
    avg = aggregator.update('Laptop', 1100.0)
    
    expected_avg = (900.0 + 1000.0 + 1100.0) / 3
    assert avg == expected_avg
    assert aggregator.total_orders == 3


def test_multiple_products():
    """Test aggregation with multiple products"""
    aggregator = PriceAggregator()
    
    aggregator.update('Laptop', 1000.0)
    aggregator.update('Mouse', 50.0)
    aggregator.update('Laptop', 1200.0)
    
    laptop_avg = aggregator.get_average('Laptop')
    mouse_avg = aggregator.get_average('Mouse')
    
    assert laptop_avg == 1100.0  # (1000 + 1200) / 2
    assert mouse_avg == 50.0


def test_get_statistics():
    """Test getting detailed statistics"""
    aggregator = PriceAggregator()
    
    aggregator.update('Keyboard', 75.0)
    aggregator.update('Keyboard', 100.0)
    aggregator.update('Keyboard', 85.0)
    
    stats = aggregator.get_statistics('Keyboard')
    
    assert stats['count'] == 3
    assert stats['average'] == pytest.approx(86.67, rel=0.01)
    assert stats['min'] == 75.0
    assert stats['max'] == 100.0


def test_overall_statistics():
    """Test overall aggregation statistics"""
    aggregator = PriceAggregator()
    
    aggregator.update('Laptop', 1000.0)
    aggregator.update('Mouse', 50.0)
    aggregator.update('Keyboard', 100.0)
    
    overall = aggregator.get_overall_statistics()
    
    assert overall['total_orders'] == 3
    assert overall['total_revenue'] == 1150.0
    assert overall['average_order_value'] == pytest.approx(383.33, rel=0.01)
    assert overall['unique_products'] == 3


def test_reset():
    """Test resetting aggregator state"""
    aggregator = PriceAggregator()
    
    aggregator.update('Laptop', 1000.0)
    aggregator.update('Mouse', 50.0)
    
    aggregator.reset()
    
    assert aggregator.total_orders == 0
    assert aggregator.total_revenue == 0.0
    assert len(aggregator.state) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
