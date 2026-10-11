# Key Technical Decisions

Each entry separates **what the code does** (verified) from **why**. The rationale is
either *stated* (written down by the author in `MEMORY.md`, the archived architecture doc,
or a commit message) or *inferred* (our reading of the code; unconfirmed).

Record new decisions here, and keep `MEMORY.md`'s decision log in step (see
[CLAUDE.md](../../CLAUDE.md#keeping-docs-current)).

| # | Decision (observed in code) | Rationale | Source |
|---|---|---|---|
| D1 | **Raw data is preserved unmodified to landing.** Duplicates, corrections and late events are all written. | Auditability, replay, simple ingestion. Dedup belongs downstream. | Stated (archived architecture doc) |
| D2 | **At-least-once ingestion** with manual Kafka commits after the local Parquet write (`enable.auto.commit=False`). | Avoid loss from committing before data is safely written. | Stated (MEMORY.md decisions log). See the caveat in [data-flows.md](data-flows.md#delivery-semantics-important): S3 delivery is not covered. |
| D3 | **Avro schemaless encoding, no Schema Registry.** The schema is loaded from a file by both sides. | Smaller than JSON, keeps a schema, no extra infrastructure. Accepted schema-drift risk. | Stated |
| D4 | **Kafka key = `symbol`.** | Per-symbol ordering. | Stated |
| D5 | **Parquet + Snappy, Hive-style `date=/symbol=` layout.** `date` is derived from `event_timestamp` (UTC), not arrival time. | Columnar compression, partition pruning, lakehouse convention. Using event-time partitioning means a late event lands in its *event* date. | Stated (format/layout). The event-time choice is inferred. |
| D6 | **Atomic local write** (hidden `.tmp` file, then `rename`) before upload. | Downstream never sees partial files. This was originally for Snowpipe. | Stated (archived doc) |
| D7 | **Time/size-based flush** (30 s or 100 k records) on a background thread. | Trade freshness against small-file count. | Stated |
| D8 | **Async S3 upload pool** (4 workers, queue 1000) with **synchronous fallback** when full. | Decouple upload latency from consumption. Backpressure instead of unbounded memory. | Stated |
| D9 | **Template-method base classes** (`BaseProducer`, `BaseConsumer`) with per-topic subclasses and a `CONSUMER_MODULE` Docker build arg. | Extensibility to new topics. | Stated (README "Adding New Topics") |
| D10 | **Configuration only through environment variables** plus `python-dotenv`. There are no config files. | Twelve-factor style, Compose-friendly. | Inferred |
| D11 | **Floci instead of LocalStack** for local S3 and Glue. | Fast start, MIT, no auth token. | Stated (MEMORY.md, commit `ba7813e`) |
| D12 | **Airflow, dbt and Snowflake archived** in favour of Spark + Iceberg. | Reduce operational complexity, move to an open lakehouse. | Stated (commit `1d83916`) |
| D13 | **Iceberg medallion layers** as separate Glue databases (`iceberg_bronze/silver/gold`), with one table per layer in `LAYER_CONFIG`. | ACID, time travel, MERGE-based dedup in silver. | Stated (MEMORY.md) |
| D14 | **Bronze accepts invalid data.** Validation failures are logged, not fatal (`landing_to_bronze.py`, step 5). | Bronze is raw. Quality gates belong in silver. | Stated (code comment) |
| D15 | **Spark jobs use flat imports** (`from config import …`) and must run with cwd `spark/jobs`. | Simplicity for a learning project. | Inferred |
| D16 | **Two Spark environments**: `local` (real S3 + filesystem Hadoop catalog) and `floci` (emulated S3 + Glue). | Learn against real data, test the production-like catalog locally. | Inferred |

## Superseded

The Snowflake-era design (Snowpipe, `RAW_TRADES`, dbt `stg_trades` incremental merge with a
10-minute lookback, Airflow DAGs) is preserved in
[`docs/archive/snowflake-era-architecture.md`](../archive/snowflake-era-architecture.md) and in git
history before `1d83916`. Two of its decisions are still relevant to the planned silver layer:

- Dedup key `trade_id`, latest `event_timestamp` wins.
- An incremental lookback window must exceed the maximum event lateness. The producer
  backdates events by up to 120 s.

## Open questions (need the author)

1. Should the bronze table be partitioned by `(date, symbol)` as MEMORY.md and the
   notebook describe, or by Iceberg hidden partitioning such as `days(event_ts)`?
2. Is the intended incremental strategy a watermark on `event_timestamp` (which misses
   late events), on file arrival, or Iceberg-side MERGE?
3. Is the `--env local` path (reading a real AWS bucket) still in use, or should `floci`
   become the default?
4. Is S3 delivery expected to be at-least-once? If yes, offset commits need to wait for
   upload completion.
