#!/bin/bash

# Initialize Git repository for submission

echo "📦 Initializing Git repository..."
echo ""

# Initialize git if not already done
if [ ! -d ".git" ]; then
    git init
    echo "✓ Git repository initialized"
else
    echo "✓ Git repository already exists"
fi
echo ""

# Add all files
echo "📝 Adding files to git..."
git add .

# Create initial commit
echo "💾 Creating initial commit..."
git commit -m "Initial commit: Kafka Order Processing System

Features:
- Avro serialization with Schema Registry
- Real-time price aggregation
- Exponential backoff retry logic
- Dead Letter Queue (DLQ) pattern
- Comprehensive monitoring and logging
- Docker Compose setup
- Complete documentation

Components:
- Order Producer (Avro)
- Order Consumer with aggregation
- DLQ Consumer for monitoring
- Price Aggregator (running averages)
- Retry Handler (exponential backoff)
- Demo scripts and tests
"

echo ""
echo "✅ Git repository ready!"
echo ""
echo "🔧 Next steps for GitHub submission:"
echo ""
echo "1. Create a new repository on GitHub"
echo ""
echo "2. Add remote and push:"
echo "   git remote add origin https://github.com/YOUR_USERNAME/kafka-assignment.git"
echo "   git branch -M main"
echo "   git push -u origin main"
echo ""
echo "3. Verify all files are pushed:"
echo "   git status"
echo ""
