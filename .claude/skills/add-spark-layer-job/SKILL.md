---
name: add-spark-layer-job
description: Build a new Spark job in spark/jobs/, especially the unimplemented bronze-to-silver dedup job and silver-to-gold aggregates, following the landing_to_bronze.py structure, config.py layer mapping and utils.py helpers. Covers dedup rules for duplicates, corrections and late events, Iceberg MERGE and partition choices, replacing the placeholder Make targets, and validation on Floci. Use for any new medallion-layer or Spark ETL work.
---

# Add a Spark layer job (silver / gold / other)

Read `spark/CLAUDE.md` first (flat imports, cwd, JAR coupling, real-AWS safety).

## Current state you're building on

- `landing_to_bronze.py` is the only job. Bronze is a **full copy of landing**, typically
  rewritten with `--mode overwrite` (K2: `append` duplicates everything).
- `config.LAYER_CONFIG` already maps `silver → iceberg_silver.trades` and
  `gold → iceberg_gold.trade_summary`. Glue DBs come from `make setup-floci-iceberg`.
- `make spark-silver-floci` / `spark-gold-floci` currently run **the bronze job** against the
  silver/gold tables (K4). Replace them as part of this work.
- `get_config("local", …)` ignores `layer` and always returns `local.default.raw_trades`.
  Either develop silver/gold for `--env floci` only, or extend the `local` branch to honour
  `LAYER_CONFIG`. The second option is a `config.py` change: keep all env logic there.

## 1. Skeleton: copy the structure, not the content

Create `spark/jobs/bronze_to_silver.py` (or `silver_to_gold.py`) mirroring
`landing_to_bronze.py`:

1. `parse_args()`: `--env {local,floci}` (default `floci` is reasonable for new jobs),
   `--mode`, `--dry-run`, `--log-level`.
2. `main() -> int` with numbered steps. Wrap **each** step in try/except (the bronze job only
   wraps steps 2–4; don't copy that gap), log the error,
   calls `spark.stop()` and returns `1`.
3. Configs: `src = get_config(args.env, "bronze")`, `dst = get_config(args.env, "silver")`.
   Both share a catalog on Floci, so one session works. Override the app name with
   `dataclasses.replace(dst.spark, app_name="bronze-to-silver")`.
4. `spark = create_spark_session(dst)`, `logger = setup_logging("bronze_to_silver", args.log_level)`.
5. Read the source with `spark.table(src.iceberg.full_table_name)`.
6. Transform (step 2 below), validate, write (step 3 below), verify, then `unpersist`, `spark.stop()`,
   `return 0`, and `if __name__ == "__main__": sys.exit(main())`.

Put reusable logic (dedup, merge) in `utils.py` as functions with an optional `logger`
parameter, which matches the existing helpers.

## 2. Silver dedup rules (from the data's actual semantics)

Raw rows per `trade_id` can be exact re-sends, corrections (later `event_timestamp`, new
price) or older versions (`event_timestamp` −10 s, later `ingestion_timestamp`). See
`docs/architecture/data-model.md`.

```python
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number

w = Window.partitionBy("trade_id").orderBy(col("event_timestamp").desc(), col("ingestion_timestamp").desc())
df_silver = df_bronze.withColumn("_rn", row_number().over(w)).filter("_rn = 1").drop("_rn")
```

- Latest `event_timestamp` wins (the author's stated rule). `ingestion_timestamp` is the
  deterministic tie-break.
- Silver is where quality gates apply. Decide with the user whether invalid rows
  (`validate_dataframe` failures) are dropped, quarantined to a separate table, or fail the job.
- Useful derived columns: `event_ts` (`timestamp_millis(event_timestamp)`, Spark ≥3.1) and
  `processed_at`.

## 3. Writing to Iceberg

**Ask the user before creating or replacing tables.** Partitioning is an open question (see
`docs/architecture/decisions.md`).

- **First version, simplest correct option:** recompute silver fully from bronze each run.
  Because bronze is itself fully rewritten, incremental reads of bronze snapshots aren't
  reliable (overwrite snapshots replace all files).
  ```python
  df_silver.writeTo(dst.iceberg.full_table_name).using("iceberg") \
      .partitionedBy(col("date")).createOrReplace()   # confirm partition spec first
  ```
- **Incremental version** (after bronze becomes append-only): `MERGE INTO` keyed on
  `trade_id`. The source must be deduplicated first, because MERGE fails when several source
  rows match one target row.
  ```sql
  MERGE INTO glue_catalog.iceberg_silver.trades t
  USING silver_updates s ON t.trade_id = s.trade_id
  WHEN MATCHED AND (s.event_timestamp > t.event_timestamp
       OR (s.event_timestamp = t.event_timestamp AND s.ingestion_timestamp > t.ingestion_timestamp))
    THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
  ```
  Any lookback window over bronze must exceed the producer's maximum lateness (120 s), plus
  your run interval.
- **Gold** (`iceberg_gold.trade_summary`): aggregates from silver only. Ideas recorded by the
  author include OHLCV bars, per-symbol daily volume and price movement. Agree on the grain
  (per minute or per day) with the user. Recompute affected `date` partitions with
  `overwritePartitions()`.

## 4. Wire up

- `Makefile`: point `spark-silver-floci` / `spark-gold-floci` at the new job, remove the
  "not yet implemented" echo, and add a `--dry-run` variant. Keep the `cd spark/jobs && python …`
  pattern.
- `spark/jobs/README.md`: add the job to the file list and CLI options.

## 5. Validate (Floci only)

```bash
python -m py_compile spark/jobs/*.py && make spark-test-config
export AWS_ENDPOINT_URL=http://localhost:4566 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
make spark-bronze-floci                                   # fresh bronze
(cd spark/jobs && python bronze_to_silver.py --env floci --dry-run)
(cd spark/jobs && python bronze_to_silver.py --env floci)
make verify-floci-iceberg
```

Report these checks (via a small PySpark snippet or `spark.sql` in the job's verify step):

- `count(*) == count(distinct trade_id)` in silver
- silver count == `count(distinct trade_id)` in bronze
- for a sample of `trade_id`s with more than one bronze row, the silver price equals the bronze
  row with max `(event_timestamp, ingestion_timestamp)`
- `DESCRIBE TABLE EXTENDED` shows the agreed partition spec

## 6. Docs

Update `docs/architecture/overview.md` (status table), `data-flows.md` §2 (new flow),
`data-model.md` (table columns and partitioning), `decisions.md` (dedup rule, partitioning,
incremental strategy, plus the MEMORY.md decisions log), and remove K4 (and K2/K3 if
addressed) from `known-issues.md`. Then run the `pipeline-reviewer` agent.
