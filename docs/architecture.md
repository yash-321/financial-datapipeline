# Project Architecture

## Project Summary

This project is a financial market data pipeline for ingesting synthetic trade data into an S3 landing bucket as Parquet files.

Current flow:

```
Producer → Kafka → Consumer → S3 Landing Bucket (Parquet)
```

Future flow (next phase):

```
S3 Landing Bucket → Spark Jobs → Iceberg Tables
```

The system currently simulates trade data for 5 symbols:

- AAPL
- GOOG
- MSFT
- AMZN
- TSLA

The pipeline intentionally includes duplicates, late-arriving data, and corrections so the downstream system can be tested against realistic financial data problems.

The project demonstrates:

- event-driven ingestion
- durable raw storage (S3 landing bucket)
- at-least-once processing
- late-data handling
- Hive-style partitioning for efficient queries

---

## Core Architecture

```text
Producer (Python)
  │
  │ Avro schemaless encoding
  │ symbol as message key
  ↓
Kafka topic: trades_topic
  │
  │ 3 partitions, per-symbol ordering
  ↓
Trade Consumer (Python)
  │
  │ Buffers by (date, symbol)
  │ Flushes every 30s or 100K records
  ↓
Local Parquet files
  │
  │ Snappy compression
  │ Hive-style layout: date={YYYY-MM-DD}/symbol={SYMBOL}/
  ↓
S3 Landing Bucket
  │
  └── [Future] Spark jobs read from here
             ↓
      Iceberg Tables (deduplication, ACID)
```

The current system prioritises reliability, replayability, and explainability over ultra-low latency.

This is not designed as a true low-latency trading system. It is a production-grade analytical data pipeline for financial market data.

---

## Data Flow

1. The Python producer generates synthetic trades using a random-walk price model
2. Trades are serialized with Avro schemaless encoding
3. Messages are sent to Kafka using symbol as the message key
4. Kafka partitions maintain per-symbol ordering
5. The Python consumer reads trades from Kafka
6. The consumer buffers records by (date, symbol)
7. Buffered records are flushed to Snappy-compressed Parquet files
8. Parquet files are uploaded to S3 landing bucket

---

## Core Design Principles

### 1. Landing Bucket keeps raw data

The S3 landing bucket is append-only and intentionally keeps duplicates, late data, and corrections.

**Reason:**
- preserves auditability
- allows replay and reprocessing
- keeps ingestion simple
- avoids losing information too early

**Trade-off:**
- downstream processing must handle deduplication
- landing bucket can grow quickly
- raw data may contain multiple versions of the same trade

Do not deduplicate before landing bucket unless the architecture is intentionally changed.

### 2. S3 is the durable replay layer

S3 is not just a temporary staging area. It is a durable raw file store.

**Reason:**
- files can be replayed
- historical backfills are possible
- ingestion and downstream processing are decoupled

**Trade-off:**
- adds latency
- introduces file-management complexity

This design favours recoverability over direct low-latency streaming.

### 3. At-least-once processing is acceptable

The system assumes duplicates may happen. This is intentional.

Kafka, consumer retries, and S3 uploads can all lead to duplicate records under failure/retry conditions.

The architecture handles this by:
- allowing duplicates into the landing bucket
- deduplication will happen in future Spark processing
- using trade_id as the merge key

Do not rely on exactly-once semantics unless the system is redesigned.

---

## Key Trade-offs

### Latency vs reliability

**Current design:**
- Kafka consumer flushes every ~30 seconds or at buffer threshold
- files are uploaded to S3 landing bucket

This creates end-to-end latency, but improves durability and replayability.

### Simplicity vs scalability

**Current system uses:**
- one Kafka broker
- replication factor 1
- one consumer service

This is simple and good for local development.

**Trade-off:**
- no broker fault tolerance
- limited consumer parallelism
- not production-ready as-is

**Production direction:**
- Kafka cluster with at least 3 brokers
- replication factor 3
- multiple consumer instances
- partition count aligned with consumer parallelism

### File batching vs freshness

The consumer batches records before writing Parquet.

**Benefits:**
- fewer small files
- better compression
- less S3 overhead

**Trade-off:**
- higher latency
- consumer has more in-memory state
- crash before flush may require Kafka replay

The current 30-second flush interval is a deliberate compromise between freshness and file efficiency.

### No Schema Registry vs faster development

Current schema management is file-based.

**Schema locations:**
- `configs/schemas/trade_schema.avsc`
- `consumers/src/trades.py` (PyArrow schema)

**Benefits:**
- simple
- no extra infrastructure
- easy to understand locally

**Trade-off:**
- schema drift risk
- changes must be manually coordinated
- no compatibility enforcement

**Future direction:**
- add Schema Registry for safer schema evolution

---

## Future Phases

### Phase 2: Spark Processing

- Spark jobs read from S3 landing bucket
- Apply deduplication logic (latest event per trade_id)
- Handle corrections and late-arriving data
- Write to Iceberg tables with ACID guarantees

### Phase 3: Iceberg Tables

- Time travel for historical queries
- Schema evolution support
- Partition pruning for efficient queries
- Compaction for optimized file sizes

---

## Docker Compose Profiles

| Profile | Services | Use Case |
|---------|----------|----------|
| (default) | zookeeper, kafka, kafka-ui, init-kafka | Local Kafka cluster |
| consumers | trade-consumer | Start consumer container |
Components
Producer

Location:

producers/src/trades.py

Responsibilities:

generate synthetic trades
simulate random-walk prices
inject realistic data issues
send records to Kafka

Current injected data issues:

duplicate_rate = 0.05
correction_rate = 0.05
late_event_rate = 0.02

Late events are currently backdated by roughly 1–120 seconds.

Important Kafka producer settings:

acks = all
batch.size = 16384
linger.ms = 5

Notes:

acks=all has limited value with one broker, but is the correct setting for a replicated Kafka setup.
The producer is synthetic and not intended to model real market microstructure perfectly.

What would break if changed:

changing the symbol list may break dbt tests that expect the current 5 symbols
changing schema fields requires updates across Avro, consumer, Snowflake, and dbt
changing the event timestamp format impacts Snowflake and dbt timestamp conversion
Kafka

Topic:

trades_topic

Current config:

partitions = 3
replication factor = 1
key = symbol
consumer group = trade_consumer_group

Reason for keying by symbol:

all trades for a symbol go to the same partition
per-symbol ordering is preserved

Trade-off:

ordering is only guaranteed within each symbol partition
hot symbols could create partition skew
consumer parallelism is capped by partition count

What would break if changed:

changing the key can break per-symbol ordering assumptions
reducing partitions would reduce parallelism
increasing partitions changes future message distribution but does not repartition historical data

Production direction:

increase broker count
increase replication factor
tune partition count based on throughput and consumer scaling
Consumer

Location:

consumers/src/trades.py

Responsibilities:

consume from Kafka
deserialize Avro records
group records by (date, symbol)
write Parquet files
upload files to S3
manage backpressure from S3 uploads

Buffering strategy:

group key = date + symbol
flush interval = 30 seconds in Docker Compose
buffer threshold = 100,000 records

Parquet settings:

compression = Snappy
row group size = 10,000

S3 layout:

trades/date=YYYY-MM-DD/symbol=SYMBOL/part-{timestamp_ms}-{uuid8}.parquet

Atomic write pattern:

write temp file
rename to final file
upload final file

This avoids Snowpipe ingesting partial files.

S3 uploader:

worker threads = 4
queue max size = 1000
multipart threshold = 8MB
retry attempts = 3
fallback = synchronous upload if queue full

What would break if changed:

changing the S3 path format can break Snowpipe path parsing
disabling atomic writes risks partial file ingestion
reducing upload queue size can increase consumer blocking
removing retries increases data loss risk during transient S3 failures

Important design note:

The consumer should remain at-least-once. Duplicate records are acceptable because Silver deduplicates.

S3

S3 acts as the raw durable file layer.

Current layout:

trades/date=YYYY-MM-DD/symbol=XYZ/*.parquet

Reason for Hive-style partitioning:

easier filtering by date and symbol
compatible with common lakehouse conventions
allows Snowflake to extract partition values from file paths

What would break if changed:

Snowpipe date extraction depends on date=YYYY-MM-DD
downstream assumptions may depend on symbol=XYZ
lifecycle policies and replay scripts may need updating

Future direction:

add S3 lifecycle rules
separate raw, quarantine, and failed-load prefixes
add replay/backfill scripts from S3 to Snowflake
Snowpipe

Pipe:

trades_pipe

Target table:

RAW_TRADES

Snowpipe is triggered by S3/SQS event notifications.

Current Bronze schema:

symbol              VARCHAR(10)   NOT NULL
trade_id            VARCHAR(50)   NOT NULL
price               FLOAT         NOT NULL
quantity            FLOAT         NOT NULL
event_timestamp     BIGINT        NOT NULL
ingestion_timestamp BIGINT        NOT NULL
trade_date          DATE
_loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
_source_file        VARCHAR(500)

Path parsing:

TRY_TO_DATE(
  REGEXP_SUBSTR(METADATA$FILENAME, 'date=([0-9-]+)', 1, 1, 'e', 1)
)

Reason for Snowpipe:

event-driven ingestion
less manual polling
lower operational effort than scheduled COPY jobs

Trade-off:

ingestion latency can vary
failures may not be obvious immediately
depends on AWS event configuration

What would break if changed:

S3 notification failure stops ingestion
bucket or prefix mismatch stops ingestion
schema mismatch causes file load failures
path format change breaks trade_date extraction
Snowflake Bronze

Table:

RAW_TRADES

Purpose:

raw landing table
preserve source records
expose file metadata
provide stable input to dbt

Bronze should not attempt to be clean.

Expected contents:

duplicates
late records
corrected versions of trades
raw event and ingestion timestamps

Important metadata:

_loaded_at
_source_file

These help with:

debugging
lineage
replay analysis
data freshness checks
dbt Silver Model

Model:

stg_trades

Purpose:

clean trade records
deduplicate by trade_id
apply correction logic
expose useful timestamp columns
provide reliable downstream analytical data

Materialization:

incremental
strategy = merge
unique_key = trade_id

Incremental lookback:

WHERE event_timestamp >= (
  SELECT MAX(event_timestamp) FROM stg_trades
) - 600000

The 600,000 ms lookback equals 10 minutes.

Reason:

late events are currently up to ~2 minutes late
10 minutes gives safety margin
incremental runs every 3 minutes

Trade-off:

reprocesses some recent data every run
costs more than a strict max timestamp filter
protects against late arrivals and timing gaps

Backfill mode:

WHERE event_timestamp >= DATEADD('hour', -24, CURRENT_TIMESTAMP())

Output columns include:

event_ts
trade_date
processed_at

What would break if changed:

reducing lookback can miss late records
changing unique_key can create duplicates or lost updates
removing deduplication exposes raw Bronze issues to downstream users
changing timestamp conversion can break time-based analysis

Important coupling:

The Airflow incremental schedule must remain comfortably below the dbt lookback window.

Current relationship:

dbt lookback = 10 minutes
incremental DAG = every 3 minutes

This is safe.

A schedule greater than the lookback window can create missed late data.

Airflow

Current executor:

LocalExecutor

DAGs:

DAG	Schedule	Purpose
stg_trades_incremental	every 3 minutes	frequent dbt run + lightweight tests
stg_trades_backfill	6AM and 6PM UTC	recovery/backfill + heavier tests

Incremental DAG responsibilities:

check Snowpipe health
check data is not stale
run dbt incremental model
run schema tests
run lightweight data tests
log metrics

Backfill DAG responsibilities:

verify data completeness
run dbt full refresh or backfill window
run schema tests
run heavier validation tests
run consistency tests
run completeness tests

Why this split exists:

frequent tests should be fast
expensive validation should not run every few minutes
backfill provides recovery from incremental edge cases

What would break if changed:

removing backfill reduces recovery from missed incremental windows
increasing incremental interval too far can exceed the lookback buffer
allowing concurrent dbt runs can cause merge conflicts or inconsistent results
LocalExecutor is not suitable for larger multi-DAG production workloads

Production direction:

add retries and alerting
externalize dbt commands/config
move to CeleryExecutor, KubernetesExecutor, MWAA, or another managed orchestrator
add SLA/freshness monitoring
Docker Compose Profiles
Profile	Services	Purpose
default	zookeeper, kafka, kafka-ui, init-kafka	local Kafka development
consumers	trade-consumer	run ingestion consumer
airflow	postgres, airflow-webserver, airflow-scheduler	run orchestration

The profile system keeps local development modular.

Examples:

docker compose up
docker compose --profile consumers up
docker compose --profile airflow up
Data Quality and Tests

Test categories:

Tag	Frequency	Purpose
incremental_validation	every 3 minutes	fast freshness/schema checks
heavy_validation	twice daily	expensive anomaly checks
consistency	twice daily	Bronze vs Silver checks
completeness	twice daily	symbol/date coverage checks

Freshness thresholds:

dbt source freshness:
  warn = 5 minutes
  error = 10 minutes

custom data freshness test:
  fail if stg_trades > 15 minutes stale

Important tests:

not null checks
unique trade_id
data freshness
expected symbols present
price anomaly checks
row count consistency
Bronze/Silver completeness

Testing philosophy:

Bronze can be messy
Silver must be clean
frequent tests should be cheap
heavy tests should run on a slower schedule
Schema Management

Current schema-related files:

configs/schemas/trade_schema.avsc
consumers/src/trades.py
scripts/snowflake_setup.sql
dbt models

These must stay aligned.

Schema change checklist:

update Avro schema
update producer serialization if needed
update consumer PyArrow schema
update Snowflake Bronze DDL
update Snowpipe COPY logic if needed
update dbt model
update dbt tests
update MEMORY.md

Safe changes:

adding nullable/optional fields
adding downstream-only derived fields

Risky changes:

renaming fields
removing fields
changing timestamp units
changing data types
changing trade_id semantics

Future direction:

introduce Schema Registry
add compatibility checks
automate schema validation in CI
Failure Scenarios
Consumer crashes

Expected behaviour:

uncommitted Kafka offsets are reprocessed
duplicate rows may be written later
Bronze may contain duplicates
Silver deduplication resolves final state

This is acceptable and consistent with at-least-once semantics.

S3 upload fails

Expected behaviour:

uploader retries
if async queue is full, consumer may block on synchronous upload
ingestion lag increases

Risk:

prolonged S3 failures can cause backpressure and delayed Kafka consumption

Mitigation:

retry logic
upload queue
monitor queue depth and consumer lag
Snowpipe fails

Expected behaviour:

files remain in S3
Bronze stops receiving new data
freshness tests should fail
backfill/replay can reload data after Snowpipe is fixed

Risk:

failure may not be obvious without monitoring

Mitigation:

Airflow Snowpipe check
dbt freshness tests
twice-daily backfill
future alerting
dbt incremental misses late data

Expected behaviour:

if late data arrives outside the lookback window, it may not be included in Silver
backfill DAG can recover depending on its window

Mitigation:

10-minute incremental lookback
24-hour backfill
tests for data completeness and consistency
Schema mismatch

Expected behaviour:

producer/consumer may fail serialization/deserialization
Snowpipe may reject files
dbt may fail if expected columns are missing

Mitigation:

keep schema files aligned
add schema validation
eventually add Schema Registry
Bottlenecks and Scaling Limits
Kafka partitions

Current partition count:

3

This caps effective consumer parallelism.

If more than 3 consumer instances are added to the same consumer group, some instances will be idle.

Scaling requires:

increasing partition count
verifying symbol distribution
monitoring partition skew
Single consumer instance

Current system uses one trade consumer.

Potential bottlenecks:

CPU during serialization/deserialization
Parquet writing
S3 upload queue
Kafka polling lag

Scaling direction:

multiple consumers in the same group
more Kafka partitions
per-symbol load monitoring
S3 upload queue

Current queue size:

1000

If S3 uploads are slower than Parquet generation:

queue fills
sync fallback blocks consumer
Kafka lag increases

Monitoring needed:

queue depth
upload latency
retry count
consumer lag
Snowpipe latency

Snowpipe is event-driven but not instant.

Latency depends on:

S3 event notifications
SQS delivery
Snowpipe scheduling
Snowflake load capacity

This means the system is near-real-time analytical, not low-latency trading infrastructure.

dbt merge cost

As data volume grows, incremental merge cost can increase.

Risk factors:

large Silver table
insufficient clustering
wide lookback windows
frequent merge schedule

Future options:

optimize clustering
use dynamic tables
split models by date
use streams/tasks
tune warehouse size
Critical Couplings

These values depend on each other:

Setting	Current Value	Coupled With
late event window	up to ~2 min	dbt lookback
dbt lookback	10 min	Airflow schedule
incremental DAG interval	3 min	dbt lookback
Kafka partitions	3	max consumer parallelism
S3 path format	date=.../symbol=...	Snowpipe parsing
trade_id	unique business key	dbt merge/dedup
Avro schema	source schema	consumer/Snowflake/dbt schemas

Do not change these values independently without checking downstream effects.

Important Configuration Values
Config	Value	Location	Why It Matters
Kafka partitions	3	init-kafka	consumer parallelism ceiling
Kafka replication factor	1	init-kafka	dev only, not fault tolerant
Flush interval	30s	Docker Compose	latency vs file count
Buffer max	100K	consumer config	memory vs throughput
S3 workers	4	S3 uploader	upload throughput
S3 queue size	1000	S3 uploader	backpressure control
dbt lookback	10 min	stg_trades	late data handling
Incremental DAG	3 min	Airflow	must be below lookback
Backfill window	24h	Airflow/dbt vars	recovery coverage
Decisions Log
Decision	Reason	Trade-off
Avro over JSON	smaller payloads, schema structure	needs schema coordination
Parquet over CSV	columnar, compressed, Snowflake-friendly	more complex than text files
S3 before Snowflake	replayability and decoupling	adds latency
Snowpipe over scheduled COPY	event-driven ingestion	depends on SQS/events
Bronze append-only	auditability	duplicates preserved
Silver merge model	handles corrections and duplicates	merge cost grows with data
10-minute lookback	protects against late events	reprocesses recent data
3-minute dbt schedule	freshness without excessive compute	not real-time
Manual Kafka commits	at-least-once processing	duplicates possible
LocalExecutor	simple local orchestration	not production scalable
Rejected or Deferred Alternatives
Direct Kafka to Snowflake

Pros:

lower latency
fewer moving parts
less file management

Cons:

weaker replay story
less transparent raw storage
tighter coupling
less useful for demonstrating lake/warehouse architecture

Status:

Deferred. Consider later with Snowpipe Streaming or Snowflake Kafka Connector.

Deduplicating in the consumer

Pros:

cleaner Bronze
fewer rows in Snowflake

Cons:

loses raw audit trail
makes consumer stateful
harder to replay corrections
risks dropping valid late/corrected events

Status:

Rejected. Dedup belongs in Silver.

Auto-commit Kafka offsets

Pros:

simpler consumer code

Cons:

possible message loss if offsets commit before files are safely written/uploaded

Status:

Rejected. Manual commits better support at-least-once behaviour.

Full-refresh dbt only

Pros:

simple correctness model

Cons:

inefficient as data grows
unnecessary compute
poor fit for frequent updates

Status:

Rejected. Incremental model with backfill is preferred.

Known Risks
Risk	Severity	Current Mitigation	Future Improvement
Single Kafka broker	High	accepted for local dev	3+ brokers, RF=3
Replication factor 1	High	accepted for local dev	increase RF
No Schema Registry	Medium	manual schema sync	add registry
Consumer single instance	Medium	enough for current scale	horizontal consumers
S3 upload backpressure	Medium	async queue + sync fallback	metrics + autoscaling
Snowpipe silent failure	Medium	freshness tests	alerting
dbt merge cost growth	Medium	incremental lookback	optimize clustering/dynamic tables
LocalExecutor	Low currently	simple DAGs	production executor
Hardcoded expected symbols	Low	stable demo set	config-driven symbols
Future Architecture Direction

Near-term improvements:

Add monitoring for:
Kafka consumer lag
S3 upload queue depth
Snowpipe freshness
dbt run status
Bronze/Silver row count drift
Add CI checks:
Python tests
dbt compile
dbt tests
schema compatibility checks
Improve schema management:
introduce Schema Registry
automate schema validation
version schemas properly
Add replay tooling:
reload S3 files into Snowflake
backfill specific date/symbol partitions
quarantine bad files
Add Gold layer:
OHLCV bars
symbol-level aggregates
daily volume metrics
price movement analytics

Medium-term improvements:

Scale Kafka:
3 brokers
replication factor 3
more partitions
better durability settings
Scale consumers:
multiple consumer instances
partition-aware scaling
consumer lag monitoring
Improve orchestration:
managed Airflow or Kubernetes-based executor
alerts
retries
SLAs
Improve Snowflake modelling:
dynamic tables
streams/tasks
better clustering
cost monitoring

Long-term options:

Evaluate Snowpipe Streaming or Kafka Connector for lower latency.
Introduce real quote data alongside trades.
Add order book or market depth simulation.
Add feature engineering for downstream ML/quant models.
Add real-time dashboarding with freshness and quality metrics.
Rules for Future Changes

When modifying this project, follow these rules:

Read this file before making architecture changes.
Do not remove raw data preservation without an explicit design decision.
Do not change timestamp semantics casually.
Do not change S3 path format without updating Snowpipe and downstream assumptions.
Do not change trade_id semantics without reviewing dbt merge logic.
Do not reduce the dbt lookback window unless late-event assumptions change.
Do not increase the incremental DAG interval beyond the lookback safety margin.
Keep Avro, consumer schema, Snowflake DDL, and dbt models aligned.
Prefer at-least-once ingestion with downstream deduplication.
Update this file whenever a major design decision changes.