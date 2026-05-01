# DataPipeline

Kafka-based trade data pipeline with Avro serialization and Parquet output.

## Quick Start

```bash
# Start Kafka cluster
docker compose up -d

# Wait for Kafka to be ready, then run producer
python src/trade_producer.py

# In another terminal, run consumer
python src/trade_consumer.py
```

## Docker Commands

```bash
docker compose up -d          # Start cluster (detached)
docker compose down           # Stop cluster
docker compose logs -f kafka  # View Kafka logs
docker compose ps             # Check service status
```

## Services

| Service   | Port  | Description                    |
|-----------|-------|--------------------------------|
| Kafka     | 9092  | Broker (localhost access)      |
| Zookeeper | 2181  | Kafka coordination             |
| Kafka UI  | 8080  | Web UI at http://localhost:8080 |

## Project Structure

```
src/
  trade_producer.py  # Generates random trade events
  trade_consumer.py  # Consumes trades, outputs to Parquet
configs/             # Avro schemas
data/                # Output data (raw/processed)
```
