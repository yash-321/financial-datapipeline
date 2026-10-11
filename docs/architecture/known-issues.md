# Known Issues, Technical Debt and Documentation Drift

Found during the codebase assessment at `d30b20f` (2026-10-11). **Verified** means it was
reproduced or exercised locally. **Code reading** means it follows directly from the
source but was not executed against a running stack.

When you fix one of these, delete its row and update any doc that references it.

## Correctness and reliability

| ID | Severity | Issue | Evidence |
|---|---|---|---|
| K1 | High | **Offsets are committed before S3 upload completes.** A permanently failed upload, or a crash with a non-empty upload queue, leaves data only on local disk, and Kafka won't redeliver it. There is no re-upload tool. | Code reading: `consumers/src/base.py` `flush()`, `consumers/src/trades.py` `process_batch()` |
| K2 | High | **Spark `--mode append` is not incremental.** It re-reads the entire landing prefix and appends everything again, duplicating bronze on every run. MEMORY.md's "watermark, idempotent" pattern is not implemented. | Code reading: `spark/jobs/utils.py` `read_landing_data()`, `landing_to_bronze.py` |
| K3 | Medium | **The bronze table is unpartitioned when the job creates it.** `saveAsTable` has no `partitionedBy`, which contradicts MEMORY.md and the notebook's `PARTITIONED BY (date, symbol)`. | Code reading: `spark/jobs/utils.py` `write_to_iceberg()` |
| K4 | Medium | **Silver/gold Make targets write raw bronze data** into `iceberg_silver.trades` and `iceberg_gold.trade_summary`. They only echo a "placeholder" note. | `Makefile` `spark-silver-floci`, `spark-gold-floci` |
| K5 | Medium | **The consumer disables S3 permanently if the bucket is missing at startup** (an unreachable Floci instead crashes and restarts the container). The README quick start creates the bucket after starting the consumer. | Code reading: `consumers/src/trades.py:73`. README steps 3–4. |
| K6 | Medium | **Bad Avro messages are silently dropped.** The error is logged, and a later offset on the same partition gets committed past them. There is no dead-letter path. | Code reading: `consumers/src/base.py` `_handle_message()` |
| K7 | Low | Double `stop()` on SIGTERM: the second `wait_for_uploads()` may wait 60 s for sentinels nobody consumes, exceeding Docker's 10 s grace period. | Code reading. A double Kafka `close()` was verified harmless (confluent-kafka 2.14). |
| K8 | Medium | `S3Uploader.upload_file`'s retry loop catches only `ClientError`, but boto3 raises `S3UploadFailedError` and connection errors are not `ClientError`, so the loop never retries. Its `Failed to upload … after N attempts` log never fires. Async path: about 12 attempts (boto3 3 × 4 passes), with a `time.sleep` blocking the worker. Sync fallback: the exception escapes into `flush()`, which re-buffers and rewrites the whole snapshot. | **Verified** (exception hierarchy, boto3 1.28.0) plus code reading: `consumers/src/uploaders/s3.py`, `consumers/src/trades.py` |
| K9 | Low | `verify_write` warns on every `--mode append` run after the first, because it compares a full source count with the table's cumulative count. | Code reading |

## Developer experience

| ID | Issue | Evidence |
|---|---|---|
| K10 | `make produce` and README step 5 fail with `FileNotFoundError` unless `TRADES_SCHEMA_PATH` is absolute. Both the code default and `.env.example` use the repo-relative path, which resolves against `producers/`. Workaround: `python -m producers.src.trades` from the repo root. | **Verified** |
| K11 | `make produce-docker` mounts only `configs/schemas`, not the producer code. `python -m producers.src.trades` cannot be found. | Code reading: `Makefile` |
| K12 | `make setup-floci-iceberg` and `verify-floci-iceberg` use the host `aws` CLI without `--endpoint-url`. Without `AWS_ENDPOINT_URL` exported, they target **real AWS**. The variable is honoured only by AWS CLI ≥ 2.13 (from AWS CLI release history; not checked in this repo). | Code reading: `Makefile` |
| K13 | `make spark-bronze-dry` (`--env local`) reads real AWS S3. There is no Floci dry-run target. | Code reading: `Makefile`, `spark/jobs/config.py` |
| K14 | `make restart` = `down` + `up`, so it doesn't bring the consumer back. | `Makefile` |
| K15 | `make clean` runs `docker system prune -f`, which prunes Docker resources beyond this project. | `Makefile` |
| K16 | `AWS_ENDPOINT_URL` is hardcoded to Floci in `docker-compose.yml`. README's "Switching to real AWS" via `.env` doesn't work for the container. | `docker-compose.yml` `trade-consumer.environment` |
| K17 | No tests, linting, type checking or CI. `spark/jobs/README.md` references a non-existent `tests/` directory. | Repository tree |
| K18 | `:latest` images (floci, floci-ui, kafka-ui) and `>=` ranges in producer/consumer requirements make builds non-reproducible. Spark requirements *are* pinned. | `docker-compose.yml`, `*/requirements.txt` |
| K19 | `scripts/check_parquet.py` needs `pandas`, which no requirements file lists. | `scripts/check_parquet.py` |
| K20 | Unused code paths: `ProducerConfig.from_env` and `ConsumerConfig.from_env` (which default flush to 20 s), plus an unused `copy` import, among others. | Code reading |

## Coupling and hardcoding

| ID | Issue |
|---|---|
| K21 | The Spark landing path hardcodes `/trades/` and ignores `S3_PREFIX`. |
| K22 | `spark/jobs/config.py` hardcodes a real AWS bucket name (including an account ID) as the `S3_BUCKET` fallback. The same bucket appears in `spark/jobs/README.md`, the notebook and MEMORY.md. It isn't a credential, but treat it as environment-specific: don't copy it into new code or docs. |
| K23 | Two Iceberg warehouses: the job uses `spark/warehouse/` and the notebook uses `<repo>/warehouse/`. Tables created in one are invisible to the other. |
| K24 | The Trade schema is restated in five places with no automated check (see [data-model.md](data-model.md#the-trade-record-one-schema-five-copies)). |

## Documentation drift

| Document | Drift |
|---|---|
| `MEMORY.md` | Says the bronze table is partitioned by `(date, symbol)` (K3). Says `date` is `STRING NOT NULL`, but Spark reads it as `DATE` and all columns are nullable (verified). Says the local catalog is SQLite (it is a Hadoop catalog). Says the incremental job is idempotent (K2). Says "code default is 20s" (that applies only to the unused `from_env`; the effective default is 30 s). Says the consumer hardcodes expected symbols (it doesn't; that was the archived dbt tests). |
| `README.md` | Shows Spark/Iceberg as "[Future]". Project structure omits `spark/`. Quick-start ordering (K5), producer command (K10), and switching to real AWS (K16). |
| `spark/jobs/README.md` | "Step 3: complete the TODOs" (there are none). References `tests/`. Says the local warehouse is a "Local Hadoop catalog" (correct), contradicting MEMORY.md. |
| `spark/setup_guide.md`, `troubleshooting_guide.md` | Written for the notebook: "Local SQLite" catalog, real-AWS-first setup. |
| `docs/archive/snowflake-era-architecture.md` | Entire Snowflake/dbt/Airflow design. Kept for history only. |
