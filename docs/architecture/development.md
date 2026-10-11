# Development, Testing and Local Infrastructure

There is **no deployment target, CI/CD pipeline, or infrastructure-as-code** in this
repository. "Infrastructure" means the Docker Compose stack plus the Floci emulator on a
developer machine. Real AWS appears only in the paths listed under [Safety](#safety-real-aws-vs-floci).

## Prerequisites

| Tool | Needed for | Notes |
|---|---|---|
| Docker + Compose v2 | Kafka, Floci, consumer | `docker compose` (not `docker-compose`) |
| Python 3.11 | producer, Spark job, scripts | The consumer image uses `python:3.11-slim`. The author uses a conda env named `datapipeline`, which is not defined in the repo. |
| Java 8/11+ | PySpark 3.5.0 | `spark/run_local.sh` checks for it |
| AWS CLI v2 | `make setup-floci-iceberg`, `verify-floci-iceberg` | Runs on the host. Needs `AWS_ENDPOINT_URL` exported. |
| Network access | first Spark run | Downloads Iceberg/Hadoop JARs from Maven |

Dependencies are per component. There is no root requirements file or lockfile:

```bash
pip install -r producers/requirements.txt   # confluent-kafka, fastavro, python-dotenv (>= ranges)
pip install -r spark/requirements.txt       # pyspark==3.5.0, pyarrow==14.0.0, pyiceberg==0.6.1, boto3==1.28.0 (pinned)
pip install -r consumers/requirements.txt   # only needed to run the consumer outside Docker
```

`scripts/check_parquet.py` additionally needs `pandas`, which no requirements file lists.

## Bringing the stack up (order matters)

The consumer checks the landing bucket **once at startup**. If the bucket doesn't exist
yet, uploads stay disabled until the container restarts. The README quick start creates
the bucket *after* starting the consumer, which only works when `./floci-data` already
has the bucket from a previous run.

```bash
cp .env.example .env            # emulator defaults; never put real secrets in .env.example
make up-floci                   # floci :4566, floci-ui :4500 (creates datapipeline_network)
make create-bucket              # s3://landing-bucket via a dockerised aws-cli
make up-consumers               # zookeeper, kafka, kafka-ui, init-kafka, floci, floci-ui, trade-consumer
python -m producers.src.trades  # run from the REPO ROOT (see note below)
make list-objects               # expect trades/date=…/symbol=…/part-….parquet after ~30 s
```

**Producer note.** `make produce` and the README's `cd producers && python -m src.trades`
fail with `FileNotFoundError` unless `TRADES_SCHEMA_PATH` is absolute. Both the code default and
`.env.example` use `configs/schemas/trade_schema.avsc`, which resolves against `producers/`. This was verified. Run from the repo root as shown, or set an absolute
`TRADES_SCHEMA_PATH`. `make produce-docker` is also broken: it mounts only
`configs/schemas` and never the `producers/` code.

### Spark against Floci (production-like catalog)

```bash
export AWS_ENDPOINT_URL=http://localhost:4566 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
make setup-floci-iceberg        # buckets landing-bucket + iceberg-warehouse, Glue DBs iceberg_{bronze,silver,gold}
make spark-bronze-floci         # writes glue_catalog.iceberg_bronze.raw_trades
make verify-floci-iceberg       # lists Glue DBs/tables and warehouse bucket
```

### Ports and UIs

| Service | Host port | Inside network |
|---|---|---|
| Kafka | `localhost:9092` | `kafka:29092` |
| Zookeeper | 2181 | `zookeeper:2181` |
| Kafka UI | http://localhost:8080 | |
| Floci (S3/Glue) | http://localhost:4566 | `http://floci:4566` |
| Floci UI | http://localhost:4500 | |
| Spark UI (while a job runs) | http://localhost:4040 | |

### Compose profiles

| Profile | Adds |
|---|---|
| (none) | zookeeper, kafka, kafka-ui, init-kafka |
| `floci` | floci, floci-ui |
| `consumers` | floci, floci-ui, trade-consumer |

`make down` stops the `consumers` profile and therefore everything. `make clean` removes
volumes and local images, **and runs `docker system prune -f`, which affects Docker
resources outside this project**. `make clean-data` deletes `./data/trades/*`.

## Command reference

`make help` lists everything. These targets exist in `Makefile` but `help` doesn't list
them: `build-consumer`, `shell-consumer`, `clean-data`. The `restart` target is `down` then
`up`, so it brings back only Kafka without the consumer.

| Goal | Command |
|---|---|
| Rebuild consumer after code change | `docker compose --profile consumers up -d --build trade-consumer` |
| Consumer logs | `make logs-consumer` |
| Inspect local landing Parquet | `python scripts/check_parquet.py` (repo root, needs pandas + pyarrow) |
| Kafka topics | `make topics` |
| Spark config self-test (no Spark) | `make spark-test-config` |
| Spark import check | `make spark-test-utils` (imports pyspark only, starts no session) |
| Spark dry run | `make spark-bronze-dry`. Note: this **reads real AWS S3** (`--env local`). |
| Spark dry run on Floci | `cd spark/jobs && python landing_to_bronze.py --env floci --dry-run` |

## Testing and validation

**No automated tests, linters, formatters, type checkers or CI exist.** `spark/jobs/README.md`
mentions `pytest tests/...`, but no `tests/` directory exists.

Until a test suite exists, "validated" means running the checks that apply to your change:

| Change touches | Minimum validation |
|---|---|
| Any Python | `python -m py_compile <files>` |
| `spark/jobs/config.py` | `make spark-test-config` |
| `spark/jobs/*` logic | `--env floci --dry-run`, then `make spark-bronze-floci` and `make verify-floci-iceberg` against a stack with data |
| Consumer / uploader | Rebuild the container, run the producer with a small `TRADE_COUNT`, then `make logs-consumer` (look for `Committed offsets` and `Uploaded`), `make list-objects` and `scripts/check_parquet.py` |
| Producer | Small `TRADE_COUNT=200` run. Check the counts it prints and Kafka UI message counts. |
| Schema | The full chain above, plus the [change-trade-schema](../../.claude/skills/change-trade-schema/SKILL.md) checklist |
| Compose/Makefile | `docker compose --profile consumers config -q` (fails if `.env` is missing, because of the consumer's `env_file`), `make -n <target>` |

If you add tests, keep them hermetic: unit-test `get_partition_keys`, `_build_s3_key`,
`serialize`/`deserialize` round-trips, `get_config` and `validate_dataframe` (with a local
`SparkSession`), and don't require Kafka or Floci. This is a recommendation. No convention
exists yet.

## Safety: real AWS vs Floci

| Command | Hits real AWS when… |
|---|---|
| `make spark-bronze`, `spark-bronze-incremental`, `spark-bronze-dry`, `./spark/run_local.sh` (default) | Always. `--env local` reads `s3a://$S3_BUCKET/trades/` from AWS with `.env` credentials. If `S3_BUCKET` is unset, `config.py` falls back to a hardcoded bucket name. |
| `make setup-floci-iceberg` | `AWS_ENDPOINT_URL` is **not** exported in your shell, or your AWS CLI is older than 2.13 (which ignores the variable; check with `aws --version`). It would then create buckets and Glue databases in whatever account your AWS CLI is configured for. |
| `make verify-floci-iceberg` | Same condition (read-only). |
| Notebooks in `spark/` | They hardcode a real bucket path. |

The Floci-only targets (`create-bucket`, `list-buckets`, `list-objects`) pass
`--endpoint-url http://floci:4566` explicitly and are safe.

## Debugging tips

- **No files in S3 but local Parquet exists:** check consumer logs for
  `S3 connection check failed`. Then `make create-bucket && docker restart trade-consumer`.
  Files already written are not re-uploaded.
- **`S3 upload disabled (S3_BUCKET not set)`:** `.env` is missing `S3_BUCKET`.
  Compose defaults it to empty.
- **Consumer reads nothing:** confirm the topic exists (`make topics`) and the producer
  targets `localhost:9092` from the host.
- **Spark `No module named 'config'`:** run from `spark/jobs`. The Make targets already `cd`.
- **Spark/Iceberg issues:** see `spark/troubleshooting_guide.md`. It was written for the
  notebook, so some catalog details (for example, "SQLite catalog") don't match the job,
  which uses a Hadoop or Glue catalog.
- **Floci health:** the compose healthcheck polls `http://localhost:4566/_localstack/health`.
