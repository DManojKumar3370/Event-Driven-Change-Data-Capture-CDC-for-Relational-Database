# Event-Driven Change Data Capture (CDC) Pipeline

A production-ready CDC service that captures real-time changes from MySQL database and publishes them as structured events to Apache Kafka using Docker containerization.

## Overview

This project implements an event-driven architecture for capturing database changes (INSERT, UPDATE, DELETE operations) and publishing them as structured CDC events to Apache Kafka. It demonstrates fundamental data engineering concepts including real-time change detection, event streaming, containerization, and fault-tolerant design.

### Key Features

- **Real-Time Change Detection**: Polls MySQL database at configurable intervals to detect INSERT, UPDATE, and DELETE operations
- **Event-Driven Architecture**: Transforms database changes into standardized JSON events with complete metadata
- **Fault-Tolerant Design**: Includes retry logic with exponential backoff and automatic reconnection
- **Production-Ready**: Comprehensive logging, environment-based configuration, and Docker containerization
- **Fully Tested**: Unit tests for event transformation and change detection using pytest

## System Architecture

```
MySQL Database → CDC Service → Apache Kafka
          ↓
        Change Detection
        Event Transform
        Kafka Producer
```

## Project Structure

```
cdc-pipeline/
├── src/
│   ├── main.py
│   ├── cdc_processor.py
│   ├── kafka_producer.py
│   └── requirements.txt
├── kafka-consumer/
│   ├── consumer.py
│   └── requirements.txt
├── tests/
│   └── test_cdc_processor.py
├── mysql/
│   └── init.sql
├── Dockerfile
├── docker-compose.yml
├── .env.example
└── README.md
```

## Quick Start

### Prerequisites

- Docker Desktop and Docker Compose
- Git for version control

### Installation

1. Clone the repository:
```bash
git clone https://github.com/YOUR_USERNAME/Event-Driven-Change-Data-Capture-CDC-for-Relational-Database.git
cd Event-Driven-Change-Data-Capture-CDC-for-Relational-Database
```

2. Create environment file:
```bash
cp .env.example .env
```

3. Start services:
```bash
docker-compose up -d
```

4. Verify services:
```bash
docker-compose ps
```

## Usage

### View CDC Service Logs
```bash
docker-compose logs -f cdc-service
```

### Test INSERT Operation
```bash
docker-compose exec mysql mysql -u root -proot_password cdc_db
```

Inside MySQL:
```sql
INSERT INTO products (name, description, price, stock) VALUES 
('Monitor', '27-inch 4K Monitor', 350.00, 25);
```

### Test UPDATE Operation
```sql
UPDATE products SET price = 999.99, stock = 35 WHERE id = 1;
```

### Test DELETE Operation
```sql
DELETE FROM products WHERE id = 2;
```

## Event Schema

```json
{
  "event_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "timestamp": "2026-02-11T20:30:45.123456Z",
  "table_name": "products",
  "operation_type": "INSERT|UPDATE|DELETE",
  "primary_keys": {
  "id": 123
  },
  "payload": {
  "old_data": null,
  "new_data": {
    "id": 123,
    "name": "Product Name",
    "price": 99.99,
    "stock": 50
  }
  }
}
```

## Configuration

Environment variables in `.env`:

- `CDC_DB_HOST`: MySQL hostname (default: mysql)
- `CDC_DB_PORT`: MySQL port (default: 3306)
- `CDC_DB_USER`: MySQL username (default: root)
- `CDC_DB_PASSWORD`: MySQL password
- `CDC_DB_NAME`: Database name (default: cdc_db)
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker address (default: kafka:9092)
- `KAFKA_TOPIC`: Topic name (default: cdc_events)
- `POLLING_INTERVAL`: Polling frequency in seconds (default: 5)

## Running Tests

```bash
docker-compose run --rm cdc-service pytest tests/ -v
```

With coverage:
```bash
docker-compose run --rm cdc-service pytest tests/ -v --cov=src
```

## Stopping Services

```bash
docker-compose down
```

Remove volumes:
```bash
docker-compose down -v
```


