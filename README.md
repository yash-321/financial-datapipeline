# Data Pipeline

Real-time trade data pipeline with Kafka streaming to S3 landing bucket.

## Architecture

```
Producer → Kafka → Consumer → S3 Landing Bucket (Parquet)
                                      ↓
                        [Future] Spark → Iceberg Tables
```

## Quick Start

```bash
# 1. Copy environment file and configure
cp .env.example .env

# 2. Start Kafka infrastructure
make up

# 3. Start consumer (writes Parquet to S3 landing bucket)
make up-consumers

# 4. Run producer locally
cd producers && pip install -r requirements.txt
python -m src.trades
```

## Commands

```bash
make up              # Start Kafka (zookeeper, kafka, kafka-ui)
make up-consumers    # Start Kafka + trade consumer
make down            # Stop all services
make logs            # Tail all logs
make ps              # Show running containers
make produce         # Run trade producer locally
make help            # Show all available commands
```

## Services

| Service       | Port  | Profile    | Description                         |
|---------------|-------|------------|-------------------------------------|
| Kafka         | 9092  | (default)  | Message broker                      |
| Zookeeper     | 2181  | (default)  | Kafka coordination                  |
| Kafka UI      | 8080  | (default)  | Web UI: http://localhost:8080       |
| Trade Consumer| -     | consumers  | Writes Parquet to S3 landing bucket |

## Project Structure

```
DataPipeline/
├── docker-compose.yml      # Compose with Kafka + consumer
├── Makefile                # Development commands
├── .env.example            # Environment template
│
├── configs/
│   └── schemas/
│       └── trade_schema.avsc   # Avro schema for trades
│
├── consumers/              # Kafka consumers (containerized)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── src/
│       ├── base.py         # BaseConsumer (extend for new topics)
│       ├── trades.py       # TradesConsumer implementation
│       └── uploaders/
│           └── s3.py       # Async S3 uploader
│
├── producers/              # Kafka producers (local scripts)
│   ├── requirements.txt
│   └── src/
│       ├── base.py         # BaseProducer (extend for new topics)
│       └── trades.py       # TradesProducer with synthetic data
│
└── scripts/                # Utility scripts
    └── check_parquet.py    # Debug Parquet files
```

## Adding New Topics

The pipeline is designed to be extensible. To add a new topic (e.g., `accounts`):

1. **Create schema**: `configs/schemas/account_schema.avsc`

2. **Create consumer**:
   ```python
   # consumers/src/accounts.py
   from .base import BaseConsumer, ConsumerConfig
   
   class AccountsConsumer(BaseConsumer):
       def get_partition_keys(self, record: dict) -> tuple:
           return (f"date={record['date']}", f"account_id={record['id']}")
       
       def process_batch(self, partition_key: str, records: list) -> Path:
           # Write parquet, upload to S3
           ...
   ```

3. **Add to docker-compose.yml**:
   ```yaml
   account-consumer:
     profiles: [consumers]
     build:
       context: ./consumers
       args:
         CONSUMER_MODULE: accounts
     environment:
       ACCOUNTS_TOPIC: accounts_topic
       # ...
   ```

4. **Create producer** (similar pattern in `producers/src/accounts.py`)

## Configuration

All configuration is via environment variables. See `.env.example` for the full list.

| Variable | Description |
|----------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address |
| `S3_BUCKET` | S3 bucket for landing zone Parquet files |
| `AWS_ACCESS_KEY_ID` | AWS credentials for S3 upload |
| `AWS_SECRET_ACCESS_KEY` | AWS credentials for S3 upload |
| `FLUSH_INTERVAL_SECONDS` | Consumer flush interval (default: 30s) |

## Future Architecture

The current pipeline writes raw Parquet files to S3 as a **landing bucket**. 

Next phase will add:
- **Spark jobs** to read from landing bucket
- **Iceberg tables** for ACID transactions and time travel
- **Deduplication** logic in Spark (currently raw data includes duplicates/corrections)

