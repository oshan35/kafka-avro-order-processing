#!/bin/bash

# Stop all Kafka services and clean up

echo "🛑 Stopping Kafka Order Processing System..."
echo ""

# Kill any running Python processes
echo "Stopping Python processes..."
pkill -f "order_consumer.py" || true
pkill -f "dlq_consumer.py" || true
pkill -f "order_producer.py" || true
echo "✓ Python processes stopped"
echo ""

# Stop Docker containers
echo "Stopping Docker containers..."
docker compose down || docker-compose down
echo "✓ Docker containers stopped"
echo ""

echo "✅ Cleanup complete!"
echo ""
echo "To remove all data (including Kafka messages):"
echo "   docker compose down -v"
echo ""
echo "To restart the system:"
echo "   ./scripts/quickstart.sh"
echo ""
