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
# 1. Copy environment file (pre-configured for local Floci emulator)
cp .env.example .env

# 2. Start Floci + Floci UI (optional, for emulator testing)
make up-floci

# 3. Start Kafka + Floci + Floci UI + consumer
make up-consumers

# 4. Create landing bucket in Floci emulator
make create-bucket

# 5. Run producer locally
cd producers && pip install -r requirements.txt
python -m src.trades

# 6. Verify uploads
make list-objects
```

## Local AWS with Floci

This project uses [Floci](https://floci.io/aws/) as a local AWS emulator for S3 uploads during development. Floci is a fast, MIT-licensed drop-in replacement for LocalStack.

**Why Floci?**
- 24ms startup, 13 MiB idle memory
- No auth token required
- Full S3 compatibility

**Architecture with Floci:**
```
Producer → Kafka → Consumer → Floci (S3 emulator) → local storage
                  ↘ Floci UI (cloud console)
                                 └── http://floci:4566 (container)
                                 └── http://localhost:4566 (host)
                └── http://localhost:4500 (Floci UI)
```

**Floci Commands:**
```bash
make create-bucket   # Create landing-bucket in Floci
make list-buckets    # List all buckets
make list-objects    # List uploaded parquet files
make logs-floci      # Tail Floci logs
make logs-floci-ui   # Tail Floci UI logs
```

**Using AWS CLI from host:**
```bash
# Set endpoint for host CLI tools
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

aws s3 ls
aws s3 ls s3://landing-bucket/ --recursive
```

**Switching to real AWS:**

Update `.env` to remove or empty `AWS_ENDPOINT_URL` and set real credentials:
```bash
AWS_ACCESS_KEY_ID=your_real_key
AWS_SECRET_ACCESS_KEY=your_real_secret
AWS_ENDPOINT_URL=
S3_BUCKET=your-production-bucket
```

## Commands

```bash
# Infrastructure
make up              # Start Kafka (zookeeper, kafka, kafka-ui)
make up-floci        # Start Floci + Floci UI
make up-floci-ui     # Start Floci UI only (requires Floci running)
make up-consumers    # Start Kafka + Floci + trade consumer
make down            # Stop all services
make down-floci      # Stop Floci + Floci UI services
make logs            # Tail all logs
make ps              # Show running containers

# Floci (AWS Emulator)
make create-bucket   # Create landing bucket in Floci
make list-buckets    # List buckets in Floci
make list-objects    # List objects in landing bucket
make logs-floci      # Tail Floci logs
make logs-floci-ui   # Tail Floci UI logs

# Producer
make produce         # Run trade producer locally
make help            # Show all available commands
```

## Services

| Service       | Port  | Profile    | Description                         |
|---------------|-------|------------|-------------------------------------|
| Kafka         | 9092  | (default)  | Message broker                      |
| Zookeeper     | 2181  | (default)  | Kafka coordination                  |
| Kafka UI      | 8080  | (default)  | Web UI: http://localhost:8080       |
| Floci UI      | 4500  | floci, consumers | Cloud console: http://localhost:4500 |
| Floci         | 4566  | floci, consumers | AWS emulator (S3): http://localhost:4566 |
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
| `AWS_ENDPOINT_URL` | Custom S3 endpoint (for Floci/LocalStack) |
| `FLUSH_INTERVAL_SECONDS` | Consumer flush interval (default: 30s) |
| `FLOCI_STORAGE_MODE` | Floci storage: memory, hybrid, persistent, wal |

## Future Architecture

The current pipeline writes raw Parquet files to S3 as a **landing bucket**. 

Next phase will add:
- **Spark jobs** to read from landing bucket
- **Iceberg tables** for ACID transactions and time travel
- **Deduplication** logic in Spark (currently raw data includes duplicates/corrections)

