# Data Model and Contracts

## The Trade record: one schema, five copies

The Avro file is the declared source of truth, but the shape is restated in several
places. Nothing checks that these stay aligned.

| # | Location | Form | Notes |
|---|---|---|---|
| 1 | `configs/schemas/trade_schema.avsc` | Avro record `com.datapipeline.trades.Trade` | Loaded at runtime by producer and consumer (`*_SCHEMA_PATH`). |
| 2 | `producers/src/trades.py` `generate()` | dict literal | Must produce every Avro field. There are no defaults or optionals in the schema. |
| 3 | `consumers/src/trades.py` `TradesConsumer.ARROW_SCHEMA` | PyArrow schema | Determines the landing Parquet column types. |
| 4 | `spark/jobs/utils.py` `write_to_iceberg()` | hardcoded `columns` list | Selects the 6 fields plus `date`. A new field is silently dropped unless added here. |
| 5 | `spark/jobs/utils.py` `validate_dataframe()` | default `required_columns` | `symbol, trade_id, price, quantity`. |

Use the `change-trade-schema` skill (`.claude/skills/change-trade-schema/`) for any change.

### Fields

| Field | Avro | Arrow (landing) | Spark read type | Semantics |
|---|---|---|---|---|
| `symbol` | string | string | string | Ticker. One of `AAPL, GOOG, MSFT, AMZN, TSLA` (`TradesProducer.DEFAULT_SYMBOLS`). Also the Kafka key and a partition directory. |
| `trade_id` | string | string | string | `uuid4`. **Not unique in raw data**: duplicates, corrections and older versions reuse it. It is the intended merge key for silver. |
| `price` | double | float64 | double | Random walk, floored at 0.01, rounded to 2 dp. |
| `quantity` | double | float64 | double | Uniform 1–500, 4 dp. |
| `event_timestamp` | long | int64 | bigint | Epoch **milliseconds**, event time. It drives the `date=` partition (UTC). It can be earlier than real time (late events) or later than the base trade (corrections). |
| `ingestion_timestamp` | long | int64 | bigint | Epoch ms, set by the producer at send time. Despite the Avro doc string it is **not** the Kafka/broker timestamp. |
| `date` | n/a | (path only) | **date** | Hive partition column discovered by Spark from `date=YYYY-MM-DD`. Spark's partition type inference yields `DateType`, which was verified locally with PySpark 3.5.0. |

All Spark-read columns are nullable. Nothing in the job enforces `NOT NULL` at the table level.

### Duplicate and correction semantics (for the silver layer)

For a given `trade_id`, raw data may contain:

- exact re-sends that differ only in `ingestion_timestamp`
- **corrections** with a *later* `event_timestamp` and a different price
- **older versions** with an `event_timestamp` 10 s *earlier* and a later `ingestion_timestamp`

The documented intent (MEMORY.md) is "latest `event_timestamp` wins per `trade_id`". Under
that rule, older versions lose even though they arrive later, and that appears to be the
intended test case. **(inferred)** The tie-break between identical `event_timestamp`s is
undefined. Prefer the latest `ingestion_timestamp` for determinism.

## Storage layouts

### Local landing (consumer container → bind mount)

```
./data/trades/date=YYYY-MM-DD/symbol=SYM/part-<epoch_ms>-<uuid8>.parquet
./data/trades/date=YYYY-MM-DD/symbol=SYM/.part-<…>.parquet.tmp   # transient
```

### S3 landing

```
s3://$S3_BUCKET/$S3_PREFIX/date=YYYY-MM-DD/symbol=SYM/part-<epoch_ms>-<uuid8>.parquet
```

Defaults: `S3_BUCKET=landing-bucket` (`.env.example`), `S3_PREFIX=trades`. The Spark job
hardcodes `/trades/` as the read prefix, so changing `S3_PREFIX` breaks Spark.

File-size profile: one file per (date, symbol) per flush. At the default rate (about 50
trades/s across 5 symbols, 30 s flush) that is roughly 300 rows per file, which means many
small files. **(inferred)** Iceberg compaction is not implemented.

### Iceberg tables

| Layer | `--env floci` table | `--env local` table | Implemented? |
|---|---|---|---|
| bronze | `glue_catalog.iceberg_bronze.raw_trades` | `local.default.raw_trades` | Yes (full-refresh copy of landing) |
| silver | `glue_catalog.iceberg_silver.trades` | n/a | No (placeholder writes raw rows) |
| gold | `glue_catalog.iceberg_gold.trade_summary` | n/a | No (placeholder writes raw rows) |

Columns written: `symbol, trade_id, price, quantity, event_timestamp, ingestion_timestamp, date`.
Format: Parquet, Snappy (`write.parquet.compression-codec`). Partitioning: **none when the job
creates the table**. Iceberg format version is not set explicitly by the job (unverified
default).

Glue databases `iceberg_bronze|silver|gold` and buckets `landing-bucket` and
`iceberg-warehouse` are created by `make setup-floci-iceberg`.

## Kafka contract

| Item | Value | Where |
|---|---|---|
| Topic | `trades_topic` | `init-kafka`, `TRADES_TOPIC` |
| Partitions / RF | 3 / 1 | `init-kafka` |
| Key | `symbol` (UTF-8) | `TradesProducer.get_key` |
| Value | Avro **schemaless** binary. No magic byte, no schema ID. | `BaseProducer.serialize` |
| Consumer group | `trade_consumer_group` | `TRADES_GROUP_ID` |
| Offset reset | `earliest` | `ConsumerConfig.auto_offset_reset` |
| Commit | manual, synchronous, after local file write | `BaseConsumer.flush` |

Because values are schemaless, a reader must have the exact writer schema. Any field change
requires deploying producer and consumer together and draining the topic, or tolerating
deserialisation failures (which are skipped, see [data-flows.md](data-flows.md#consumer-consumerssrc)).

## Configuration contract (environment variables)

The precedence for the consumer container is: `environment:` in `docker-compose.yml`
overrides `env_file: .env`, which overrides code defaults.

| Variable | Used by | Default (code) | Compose value | Notes |
|---|---|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | producer, consumer | `localhost:9092` / `kafka:29092` | `kafka:29092` (hardcoded) | |
| `TRADES_TOPIC` | producer, consumer | `trades_topic` | hardcoded | |
| `TRADES_GROUP_ID` | consumer | `trade_consumer_group` | hardcoded | |
| `TRADES_SCHEMA_PATH` | producer, consumer | relative / `/app/configs/...` | `/app/configs/schemas/trade_schema.avsc` | Relative paths resolve against the **cwd**. |
| `TRADES_OUTPUT_DIR` | consumer | `/app/data/trades` | same | |
| `FLUSH_INTERVAL_SECONDS` | consumer | `30` | `30` | `ConsumerConfig.from_env` would default to 20, but it is unused. |
| `MAX_BUFFER_SIZE` | consumer | `100000` | n/a | |
| `S3_BUCKET` | consumer, Spark `--env local` | none (S3 off) / a hardcoded real bucket | `${S3_BUCKET:-}` | Empty means uploads are disabled. |
| `S3_PREFIX` | consumer | `trades` | `${S3_PREFIX:-trades}` | Spark ignores it. |
| `AWS_ENDPOINT_URL` | consumer | none | `http://floci:4566` (**hardcoded**) | `.env` cannot point the container at real AWS. |
| `AWS_REGION` | consumer, Spark local | `us-east-1` / `eu-west-1` | `${AWS_REGION:-us-east-1}` | |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | consumer, Spark local | n/a | `${…:-test}` | Never commit real values. |
| `DELETE_LOCAL_AFTER_UPLOAD` | consumer | `false` | `${…:-false}` | |
| `TRADE_COUNT`, `TRADE_DELAY` | producer | `10000`, `0.02` | n/a | |
| `FLOCI_BUCKET`, `FLOCI_WAREHOUSE_BUCKET` | Spark `--env floci` | `landing-bucket`, `iceberg-warehouse` | n/a | Not in `.env.example`. |
| `FLOCI_STORAGE_MODE` | floci container | `hybrid` | `${…:-hybrid}` | `memory`, `hybrid`, `persistent` or `wal`. |
