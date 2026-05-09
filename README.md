# Data Pipeline

Real-time trade data pipeline with Kafka, S3, Snowflake, dbt, and Airflow.

## Architecture

```
Producer → Kafka → Consumer → S3 (Parquet) → Snowpipe → Snowflake → dbt → Silver Layer
                                                                      ↓
                                                              Airflow (orchestration)
```

## Quick Start

```bash
# 1. Copy environment file and configure
cp .env.example .env

# 2. Start Kafka infrastructure
make up

# 3. Start consumer (in separate terminal or with profile)
make up-consumers

# 4. Run producer locally
cd producers && pip install -r requirements.txt
python -m src.trades

# 5. (Optional) Start Airflow for orchestration
make up-airflow
```

## Commands

```bash
make up              # Start Kafka (zookeeper, kafka, kafka-ui)
make up-consumers    # Start Kafka + trade consumer
make up-airflow      # Start full stack including Airflow
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
| Trade Consumer| -     | consumers  | Writes Parquet to S3                |
| Airflow       | 8081  | airflow    | Web UI: http://localhost:8081       |

## Project Structure

```
DataPipeline/
├── docker-compose.yml      # Unified compose with profiles
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
├── airflow/                # Airflow orchestration
│   ├── Dockerfile
│   ├── dags/               # DAG definitions
│   ├── dbt_profiles/       # dbt connection profiles
│   └── utils/              # Shared utilities
│
├── dbt/
│   └── market_data_platform/   # dbt project
│       ├── models/silver/      # Staging models
│       └── tests/              # Data quality tests
│
└── scripts/                # Utility scripts
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
| `S3_BUCKET` | S3 bucket for parquet files |
| `SNOWFLAKE_*` | Snowflake connection details |
| `AIRFLOW_UID` | Airflow user ID (default: 50000) |

