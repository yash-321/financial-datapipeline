---
name: run-local-pipeline
description: Bring up the local Docker/Floci stack in the correct order, run an end-to-end smoke test (producer → Kafka → consumer → S3 landing → optional Spark → Iceberg on Floci), and diagnose failures such as "no files in S3", "consumer reads nothing" or "Spark can't find the table". Use when asked to run, test or verify the pipeline, or when validating ingestion or Spark changes against real services.
---

# Run and smoke-test the pipeline locally

Everything here runs on the developer machine. Steps marked **⚠ real AWS** must not run
without the user's explicit OK.

## 0. Preconditions

```bash
docker info >/dev/null && echo docker-ok
test -f .env || cp .env.example .env     # never print or edit secrets in .env
python -c "import confluent_kafka, fastavro, dotenv" # producer deps (producers/requirements.txt)
```

The author's environment is a conda env named `datapipeline`. Use it if it exists
(`conda run -n datapipeline …` or an activated shell).

## 1. Start services (order matters)

The consumer runs one `head_bucket` check at startup and disables S3 uploads permanently if
the bucket is missing. Create the bucket **before** starting the consumer.

```bash
make up-floci                 # floci :4566 + floci-ui :4500; creates datapipeline_network
make create-bucket            # s3://landing-bucket (Floci-only, explicit endpoint)
make up-consumers             # kafka stack + trade-consumer
make ps                       # all Up; init-kafka exits after creating trades_topic
make logs-consumer            # expect "S3 connection verified" and "Started 4 S3 upload workers"
```

`make create-bucket` prints an error if the bucket already exists (Floci persists to
`./floci-data`). That's fine.

After consumer code changes: `docker compose --profile consumers up -d --build trade-consumer`.

## 2. Produce a small batch

Run from the **repo root**. `make produce` fails with the example `.env` (K10).

```bash
TRADE_COUNT=200 TRADE_DELAY=0.01 python -m producers.src.trades
```

Expected output: a summary with trades, duplicates, corrections and late events, where total
sent is roughly 1.12 × trades.

## 3. Verify landing (wait ≥ `FLUSH_INTERVAL_SECONDS`, default 30 s)

```bash
make logs-consumer            # "Processed N records -> …", "Committed offsets…", "Uploaded part-…"
make list-objects             # trades/date=YYYY-MM-DD/symbol=SYM/part-<ms>-<uuid8>.parquet
python scripts/check_parquet.py   # local copy in ./data/trades (needs pandas + pyarrow)
```

Pass criteria: files for all five symbols, row totals roughly equal to the producer's
"Total sent", and no `Failed to process message`, `Upload failed for`, `Failed to flush buffer` or `Queue full` in the logs.

## 4. Optional: Spark → Iceberg on Floci

Export the emulator endpoint **in the same command** as the AWS CLI targets. Without it,
they hit real AWS.

```bash
export AWS_ENDPOINT_URL=http://localhost:4566 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
make setup-floci-iceberg      # buckets + Glue DBs iceberg_{bronze,silver,gold}
(cd spark/jobs && python landing_to_bronze.py --env floci --dry-run)
make spark-bronze-floci       # first run downloads JARs from Maven
make verify-floci-iceberg     # expect raw_trades in iceberg_bronze
```

Pass criteria: the log says `Counts match - write verified`, and the job exits 0.

**⚠ real AWS**: `make spark-bronze`, `spark-bronze-incremental`, `spark-bronze-dry`,
`./spark/run_local.sh` without `--env floci`. Don't run these unasked.

## 5. Teardown

```bash
make down                     # stops everything; data persists in ./data and ./floci-data
```

`make clean` also runs `docker system prune -f` (system-wide), and `make clean-data` deletes
`./data/trades/*`. Ask before running either.

## Troubleshooting

| Symptom | Likely cause → fix |
|---|---|
| `S3 connection check failed` / no uploads, but local Parquet exists | Bucket missing at consumer start → `make create-bucket && docker restart trade-consumer`. Earlier files are **not** re-uploaded. Copy them manually if needed. |
| `S3 upload disabled (S3_BUCKET not set)` | `.env` lacks `S3_BUCKET=landing-bucket` |
| Producer `FileNotFoundError: configs/schemas/trade_schema.avsc` | Run from the repo root, or set an absolute `TRADES_SCHEMA_PATH` |
| Producer `Connect to …9092 failed` | Kafka not up (`make ps`), or `KAFKA_BOOTSTRAP_SERVERS` not `localhost:9092` on the host |
| Consumer up but no `Processed` lines | Wait for the flush interval. Check `make topics` for `trades_topic`, and the consumer group in Kafka UI :8080. |
| `Failed to process message` | Producer and consumer Avro schemas differ, or old-schema messages remain (see the `change-trade-schema` skill) |
| `Queue full, sync uploading` | Floci slow or down. Check `make logs-floci`. |
| `make create-bucket` network error | `datapipeline_network` doesn't exist yet. Run `make up-floci` first. |
| Spark `No module named 'config'` | Run from `spark/jobs` |
| Spark `NoSuchBucket` / empty read on Floci | No landing data yet (steps 2–3), or `make setup-floci-iceberg` not run |
| Glue `EntityNotFoundException` / database not found | `make setup-floci-iceberg` was run without `AWS_ENDPOINT_URL` (it went to real AWS or failed). Re-run with the export. |
| Spark hangs at startup | Downloading JARs from Maven. Needs network. |

More Spark/Iceberg errors: `spark/troubleshooting_guide.md` (notebook-oriented).
