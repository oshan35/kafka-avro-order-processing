# Manual Setup & Run Instructions

## Prerequisites
- Docker and Docker Compose installed
- Python 3.9+ with pip
- Terminal access

## Steps to Run

### 1. Start Kafka Infrastructure
```bash
docker compose up -d
```
Wait ~30 seconds for services to be ready.

### 2. Create Kafka Topics
```bash
bash scripts/setup_topics.sh
```

### 3. Install Python Dependencies
```bash
pip install -r requirements.txt
```

### 4. Run the System

**Terminal 1 - Start Consumer:**
```bash
python src/consumer/order_consumer.py
```
This will process orders and show real-time aggregation statistics.

**Terminal 2 - Start DLQ Monitor (Optional):**
```bash
python src/consumer/dlq_consumer.py
```
This monitors failed messages sent to the Dead Letter Queue.

**Terminal 3 - Run Producer:**
```bash
python src/producer/order_producer.py
```
This generates 30 sample orders and sends them to Kafka.

### 5. View Results
- **Consumer Terminal**: See order processing with running averages per product
- **DLQ Terminal**: See any failed messages (simulated ~5% failure rate)
- **Kafka UI**: Open http://localhost:8080 to view topics and messages

### 6. Stop Everything
```bash
docker compose down
```

