#!/usr/bin/env python3
"""
Price Aggregator
Maintains running averages of product prices in real-time
"""

from collections import defaultdict
from dataclasses import dataclass
from typing import Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class AggregationState:
    count: int = 0
    sum: float = 0.0
    avg: float = 0.0
    min_price: float = float('inf')
    max_price: float = float('-inf')


class PriceAggregator:
    
    def __init__(self):

        self.state: Dict[str, AggregationState] = defaultdict(AggregationState)
        self.total_orders = 0
        self.total_revenue = 0.0
    
    def update(self, product: str, price: float) -> float:

        state = self.state[product]
        
        # Update statistics
        state.count += 1
        state.sum += price
        state.avg = state.sum / state.count
        state.min_price = min(state.min_price, price)
        state.max_price = max(state.max_price, price)
        
        # Update totals
        self.total_orders += 1
        self.total_revenue += price
        
        return state.avg
    
    def get_average(self, product: str) -> float:

        return self.state[product].avg if product in self.state else 0.0
    
    def get_statistics(self, product: str) -> Dict:

        if product not in self.state:
            return {
                'count': 0,
                'sum': 0.0,
                'average': 0.0,
                'min': 0.0,
                'max': 0.0
            }
        
        state = self.state[product]
        return {
            'count': state.count,
            'sum': round(state.sum, 2),
            'average': round(state.avg, 2),
            'min': round(state.min_price, 2),
            'max': round(state.max_price, 2)
        }
    
    def get_all_statistics(self) -> Dict[str, dict]:

        return {
            product: self.get_statistics(product)
            for product in self.state.keys()
        }
    
    def get_overall_statistics(self) -> Dict:

        avg_order_value = self.total_revenue / self.total_orders if self.total_orders > 0 else 0.0
        
        return {
            'total_orders': self.total_orders,
            'total_revenue': round(self.total_revenue, 2),
            'average_order_value': round(avg_order_value, 2),
            'unique_products': len(self.state)
        }
    
    def print_summary(self):
        logger.info("\n" + "=" * 80)
        logger.info("📊 PRICE AGGREGATION SUMMARY")
        logger.info("=" * 80)
        
        # Overall statistics
        overall = self.get_overall_statistics()
        logger.info(f"\n🔢 Overall Statistics:")
        logger.info(f"   Total Orders: {overall['total_orders']}")
        logger.info(f"   Total Revenue: ${overall['total_revenue']:.2f}")
        logger.info(f"   Average Order Value: ${overall['average_order_value']:.2f}")
        logger.info(f"   Unique Products: {overall['unique_products']}")
        
        # Per-product statistics
        logger.info(f"\n📦 Per-Product Statistics:")
        logger.info("-" * 80)
        logger.info(f"{'Product':<20} {'Count':>8} {'Average':>12} {'Min':>12} {'Max':>12}")
        logger.info("-" * 80)
        
        # Sort by count (most popular first)
        sorted_products = sorted(
            self.state.items(),
            key=lambda x: x[1].count,
            reverse=True
        )
        
        for product, state in sorted_products:
            logger.info(
                f"{product:<20} {state.count:>8} "
                f"${state.avg:>11.2f} ${state.min_price:>11.2f} ${state.max_price:>11.2f}"
            )
        
        logger.info("=" * 80 + "\n")
    
    def reset(self):
        """Reset all aggregation state"""
        self.state.clear()
        self.total_orders = 0
        self.total_revenue = 0.0
        logger.info("Aggregator state reset")


# Example usage
if __name__ == "__main__":
    # Create aggregator
    aggregator = PriceAggregator()
    
    # Sample data
    test_orders = [
        ('Laptop', 999.99),
        ('Mouse', 25.50),
        ('Laptop', 1099.99),
        ('Keyboard', 75.00),
        ('Laptop', 899.99),
        ('Mouse', 29.99),
        ('Monitor', 349.99),
        ('Keyboard', 89.99),
        ('Monitor', 299.99),
        ('Mouse', 19.99),
    ]
    
    # Process orders
    for product, price in test_orders:
        avg = aggregator.update(product, price)
        print(f"Added {product} @ ${price:.2f} -> Running avg: ${avg:.2f}")
    
    # Print summary
    aggregator.print_summary()
