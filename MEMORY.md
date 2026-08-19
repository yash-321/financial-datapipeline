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
| consumers | trade-consumer | Start consumer container |

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

---

## Future Considerations

1. **Horizontal scaling:** Multiple consumer instances need Kafka partition count ≥ instance count
2. **Schema evolution:** Adding optional Avro fields is safe; removing/renaming requires consumer version coordination
3. **Spark processing:** Read from landing bucket, deduplicate, write to Iceberg tables
4. **Deduplication:** Future Spark jobs will use trade_id as merge key with latest event_timestamp wins

---

## Archived Components (git history)

The following components were removed in Aug 2026 for transition to Spark/Iceberg:

- **Airflow** (`airflow/`): DAGs for orchestrating dbt runs
- **dbt** (`dbt/`): Incremental models for Snowflake transformations
- **Snowflake**: Bronze/Silver layer with Snowpipe ingestion
- **Scripts**: `snowflake_setup.sql`, `check_snowpipe_status.py`

These are preserved in git history for reference.
