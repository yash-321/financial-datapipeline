# Project Memory

## Overview

Trade data pipeline: Producer → Kafka → Consumer → S3 Landing Bucket (Parquet)

**Scope:** 5 symbols (AAPL, GOOG, MSFT, AMZN, TSLA), synthetic price data via random-walk generator.

**Future:** Spark jobs will read from landing bucket and write to Iceberg tables.

---

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌──────────────────┐
│  Producer   │────▶│    Kafka    │────▶│  Consumer   │────▶│  S3 Landing      │
│  (Python)   │     │ (1 broker)  │     │  (Python)   │     │  Bucket          │
│             │     │ 3 partitions│     │ 4 S3 workers│     │  (Parquet)       │
└─────────────┘     └─────────────┘     └─────────────┘     └────────┬─────────┘
      │                   │                   │                      │
      │ Avro              │ symbol key        │ 30s flush            │
      │ schemaless        │ → ordering        │ or 100K buffer       ▼
      │                   │                   │               ┌─────────────┐
      │                   │                   │               │  [Future]   │
      │                   │                   │               │ Spark Jobs  │
      │                   │                   │               │     ↓       │
      │                   │                   │               │ Iceberg     │
      │                   │                   │               │ Tables      │
      │                   │                   │               └─────────────┘
```

### Docker Compose Profiles

| Profile | Services | Use Case |
|---------|----------|----------|
| (default) | zookeeper, kafka, kafka-ui, init-kafka | Local Kafka cluster |
| floci | floci, floci-ui | Start local AWS emulator + Floci UI |
| consumers | floci, floci-ui, trade-consumer | Start consumer stack with local S3 emulator + cloud console |

---

## Component Details

### 1. Producer (`producers/src/trades.py`)

Generates synthetic trades with random-walk price movement.

**Key config (ProducerConfig):**
- `acks=all` — wait for all replicas (currently just 1)
- `batch.size=16384` — batch up to 16KB before send
- `linger.ms=5` — wait up to 5ms for batching

**Test event injection (for dedup testing):**
- `duplicate_rate=0.05` — 5% duplicate trade_ids
- `correction_rate=0.05` — 5% price corrections (same trade_id, newer timestamp)
- `late_event_rate=0.02` — 2% backdated events (1-120 seconds late)

**What would break if changed:**
- Removing `acks=all`: Could lose messages if broker fails before replication
- Changing symbol list: Consumer hardcodes expected symbols `['AAPL', 'AMZN', 'GOOG', 'MSFT', 'TSLA']`
- Schema field changes: Requires updating `configs/schemas/trade_schema.avsc` and consumer PyArrow schema

### 2. Kafka (`trades_topic`)

- 3 partitions, replication factor 1
- Messages keyed by `symbol` → all trades for same symbol go to same partition
- Consumer group: `trade_consumer_group`

**What would break if changed:**
- Adding partitions: Existing consumer would need rebalance; new partition won't have history
- Changing key: Would break per-symbol ordering guarantees

### 3. Consumer (`consumers/src/trades.py`)

Reads from Kafka, buffers records, writes Parquet, uploads to S3 landing bucket.

**Buffering strategy:**
- Groups records by `(date, symbol)` partition key
- Flushes when: 30 seconds elapsed OR buffer hits 100K records
- Code default is 20s (`ConsumerConfig`), but docker-compose overrides to 30s

**Parquet output:**
- Compression: Snappy
- Row group size: 10,000
- Path: `data/trades/date={YYYY-MM-DD}/symbol={SYMBOL}/part-{timestamp_ms}-{uuid8}.parquet`
- Atomic write: temp file (`.filename.tmp`) → rename

**S3 upload (`S3Uploader`):**
- 4 async worker threads (configurable)
- Queue max size: 1000 files
- If queue full: falls back to synchronous upload (blocks consumer)
- Multipart threshold: 8MB
- Retry: 3 attempts with exponential backoff (boto3 adaptive mode)
- Preserves Hive partition structure in S3 key

**What would break if changed:**
- Queue max size reduced: More sync upload fallbacks, consumer latency spikes
- Disabling atomic writes: Downstream could read partial files
- Changing partition path format: Breaks Hive-style partitioning expectations

---

## Critical Configuration Values

| Config | Value | Location | Impact of Change |
|--------|-------|----------|------------------|
| Kafka partitions | 3 | docker-compose `init-kafka` | Consumer parallelism ceiling |
| Flush interval | 30s | docker-compose `FLUSH_INTERVAL_SECONDS` | Latency vs file count tradeoff |
| Buffer max | 100K | consumers/src/base.py `max_buffer_size` | Memory usage |
| S3 queue size | 1000 | consumers/src/uploaders/s3.py | Overflow → sync fallback |

---

## File Organization

```
configs/schemas/trade_schema.avsc     # Avro schema (source of truth)
consumers/src/trades.py               # PyArrow schema must match
```

Schema changes require updating both locations.

---

## Known Risks

| Risk | Severity | Mitigation |
|------|----------|------------|
| Single Kafka broker (RF=1) | High | Acceptable for dev; prod needs ≥3 brokers |
| Consumer single instance | Medium | Current throughput handles 5 symbols; scaling needs partition tuning |
| S3 queue overflow | Medium | Sync fallback exists; monitor queue depth |
| No schema registry | Low | Avro schema file works; breaking changes need coordination |

---

## Decisions Log

| When | Decision | Why | Alternative Rejected |
|------|----------|-----|----------------------|
| Initial | Avro over JSON | 40% smaller, schema evolution | JSON: no schema, larger |
| Initial | Parquet over CSV | Columnar, better compression | CSV: larger, slower queries |
| Initial | Manual Kafka commits | At-least-once semantics | Auto-commit: at-most-once risk |
| Aug 2026 | Archive Airflow/dbt/Snowflake | Transition to Spark/Iceberg | Keep: operational complexity |
| Aug 2026 | Floci for local S3 | Fast (24ms), MIT-licensed, drop-in | LocalStack: auth token required, heavier |

---

## Future Considerations

1. **Horizontal scaling:** Multiple consumer instances need Kafka partition count ≥ instance count
2. **Schema evolution:** Adding optional Avro fields is safe; removing/renaming requires consumer version coordination
3. **Spark processing:** Read from landing bucket, deduplicate, write to Iceberg tables
4. **Deduplication:** Future Spark jobs will use trade_id as merge key with latest event_timestamp wins

---

---

## Spark + Iceberg Layer (Bronze)

### Overview

Spark jobs transform landing bucket Parquet files → Iceberg Bronze layer (raw data tier).

**Architecture:**
```
S3 Landing (Parquet)  →  Spark  →  Iceberg Bronze Table
s3://.../trades/           ↓        local.default.raw_trades
                      Validate
                        & enrich
```

### Iceberg Table Spec

**Table:** `local.default.raw_trades`

**Location:** `s3://financial-data-warehouse-587129419094-eu-west-1/warehouse/iceberg/raw_trades/`

**Schema:**
```
symbol                 STRING NOT NULL  (stock ticker)
trade_id               STRING NOT NULL  (unique identifier)
price                  DOUBLE NOT NULL  (trade price)
quantity               DOUBLE NOT NULL  (trade quantity)
event_timestamp        LONG NOT NULL    (event time ms)
ingestion_timestamp    LONG NOT NULL    (kafka ingestion ms)
date                   STRING NOT NULL  (partition: YYYY-MM-DD)
```

**Partitioning:** By `date`, then `symbol`
- Pruning: `WHERE date = '2026-05-09' AND symbol = 'AAPL'` skips other folders
- Physical layout: `date=YYYY-MM-DD/symbol=TICKER/part-*.parquet`

**Format:**
- Compression: Snappy
- Row group size: Default (128 MB)
- Iceberg version: 2 (supports ACID updates)

### Why Iceberg (vs Parquet)

| Requirement | Parquet | Iceberg |
|-------------|---------|---------|
| ACID writes | ❌ | ✅ |
| Deduplication (merge) | ❌ | ✅ |
| Schema evolution | ⚠️ Manual | ✅ Auto |
| Time travel (snapshots) | ❌ | ✅ |
| Partition pruning | Manual | ✅ Hidden |

**Decision:** Iceberg chosen for production readiness + deduplication in Silver layer.

### Catalog Options

**Local (Dev):** `./warehouse` SQLite catalog
- File: `warehouse/metadata/catalog.db`
- Use for: Development, testing, CI/CD
- Pros: No AWS required, fast iteration
- Cons: Single machine only

**AWS Glue (Prod):** Centralized metadata
- See: `spark/glue_catalog_setup.md`
- Use for: Multi-team, cross-service access
- Pros: Central catalog, RBAC, multi-region
- Cons: AWS account required

### Spark Job Pattern (Incremental)

```python
# 1. Load configuration
last_ts = read_config('last_processed_timestamp')

# 2. Read landing: only NEW data
df_new = spark.read.parquet('s3a://...landing/trades/') \
    .filter(f'event_timestamp > {last_ts}')

# 3. Validate
assert df_new.filter('trade_id IS NULL').count() == 0

# 4. Write to Bronze (append mode)
df_new.write.format("iceberg").mode("append").saveAsTable("bronze_table")

# 5. Update watermark
new_ts = df_new.agg(max('event_timestamp')).collect()[0][0]
write_config('last_processed_timestamp', new_ts)
```

**Idempotent:** Safe to re-run if job fails (same data not duplicated).

### Learning Notebook

**File:** `spark/landing_to_bronze.ipynb`

**12 sections:**
1. Environment & Spark Session setup
2. Parquet exploration in S3
3. Read all landing data recursively
4. Data quality validation
5. Understanding Iceberg format
6. Create Iceberg table
7. Write landing → Bronze
8. Validation & querying
9. Incremental patterns (design)
10. Error handling & edge cases
11. Performance optimization
12. Next steps (Silver layer)

**Time:** 45-60 minutes to complete

**Catalog:** Uses local SQLite (no AWS needed)

### Catalog Configuration

**Local (in notebook):**
```python
spark = SparkSession.builder \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hive") \
    .config("spark.sql.catalog.local.warehouse", "./warehouse") \
    .getOrCreate()
```

**Production (Glue):**
```python
.config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
.config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
.config("spark.sql.catalog.glue_catalog.warehouse", "s3://bucket/warehouse") \
```

### Dependencies

**File:** `spark/requirements.txt`
```
pyspark==3.5.0
pyarrow==14.0.0
pyiceberg==0.6.1
boto3==1.28.0
python-dotenv==1.0.0
```

### Key Decisions

| Decision | Why | Alternative Rejected |
|----------|-----|----------------------|
| Iceberg format | ACID + dedup + time travel | Parquet: no transactions |
| Local SQLite (dev) | No AWS needed for learning | Glue: overkill for notebook |
| Partition by (date, symbol) | Matches landing structure + common queries | Symbol only: slower date filters |
| Append mode (incremental) | Efficient, idempotent | Overwrite: wastes compute |
| Snappy compression | Fast read speed balance | GZIP: slower; none: larger files |

### Common Operations

```python
# Query table
spark.sql("SELECT COUNT(*) FROM local.default.raw_trades").show()

# Incremental append
df.write.format("iceberg").mode("append").saveAsTable("table")

# Merge for deduplication (Silver layer)
# See: spark/landing_to_bronze.ipynb Section 9

# Time travel (query past snapshot)
spark.sql("""
    SELECT * FROM local.default.raw_trades
    VERSION AS OF 1  -- version 1
""")

# Drop and recreate
spark.sql("DROP TABLE local.default.raw_trades")
```

### What Would Break If Changed

| Change | Impact | Fix |
|--------|--------|-----|
| Partition order flipped | Query patterns change | Update where clauses |
| Catalog name changed | Spark config must update | See: .config("spark.sql.catalog...") |
| S3 warehouse path changed | Metadata lost, table inaccessible | Copy warehouse/, update config |
| Iceberg version v1 → v2 | Can't use merge/upsert | Accept v2 as requirement |
| Column type changed | Schema mismatch on write | Recreate table with new schema |

### Future Enhancements

1. **Silver layer:** Deduplicate + enrich (transforms Bronze → Silver)
2. **Gold layer:** Aggregate metrics (Silver → Gold)
3. **Real-time:** Kafka → Spark Streaming → Iceberg (append-only)
4. **Multi-region:** Replicate warehouse to other regions
5. **Schema evolution:** Handle adding/removing columns safely
6. **Data sharing:** Export to Delta/Snowflake/BigQuery

---

## Archived Components (git history)

The following components were removed in Aug 2026 for transition to Spark/Iceberg:

- **Airflow** (`airflow/`): DAGs for orchestrating dbt runs
- **dbt** (`dbt/`): Incremental models for Snowflake transformations
- **Snowflake**: Bronze/Silver layer with Snowpipe ingestion
- **Scripts**: `snowflake_setup.sql`, `check_snowpipe_status.py`

These are preserved in git history for reference.
