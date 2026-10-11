# DataPipeline: Claude Code Instructions

Local lakehouse-style pipeline for synthetic trade data:
**producer → Kafka → consumer → Parquet in S3 landing (Floci emulator) → Spark → Iceberg bronze**.
Silver (dedup) and gold layers are planned but **not implemented**. There is no production
deployment, CI, or test suite. Everything runs locally via Docker Compose.

Start with [docs/architecture/README.md](docs/architecture/README.md). Before changing behaviour,
read [known-issues.md](docs/architecture/known-issues.md). Several docs (MEMORY.md, README,
spark guides) disagree with the code, and the code wins.

## Repository map

| Path | What | Runs where |
|---|---|---|
| `producers/src/` | `BaseProducer` + `TradesProducer` (synthetic trades, Avro, injects dup/correction/late events) | host |
| `consumers/src/` | `BaseConsumer` + `TradesConsumer` (buffer → Parquet → commit → async S3 upload), `uploaders/s3.py` | Docker (`trade-consumer`) |
| `configs/schemas/trade_schema.avsc` | Trade Avro schema, the declared contract | mounted into consumer |
| `spark/jobs/` | Landing → Iceberg job (`landing_to_bronze.py`, `config.py`, `utils.py`). Has its own `CLAUDE.md`. | host (PySpark + Java) |
| `spark/*.ipynb`, `spark/*.md` | Learning notebook and notebook-era guides | host |
| `docker-compose.yml`, `Makefile` | Local infra (Kafka, Floci S3/Glue, consumer) and all dev commands | |
| `scripts/check_parquet.py` | Inspect local landing Parquet | host |
| `learning/` | Unrelated pandas practice material. Ignore unless asked. | |
| `docs/architecture/` | Verified architecture docs. `docs/archive/` holds superseded designs. | |
| `MEMORY.md`, `.github/copilot-instructions.md` | Author's project notes (also read by Copilot) | |

## Essential commands

Full reference and start-up order: [development.md](docs/architecture/development.md).

```bash
cp .env.example .env                 # emulator defaults
make up-floci && make create-bucket  # bucket BEFORE consumer: it checks S3 once at startup
make up-consumers                    # Kafka + Floci + trade-consumer
python -m producers.src.trades       # from repo root. `make produce` is broken (K10)
make list-objects                    # verify landing files
make logs-consumer
docker compose --profile consumers up -d --build trade-consumer   # after consumer code changes
make spark-test-config               # Spark config self-test (no JVM)
```

There is no lint, format, type-check or test command. Minimum validation per change type
is in [development.md#testing-and-validation](docs/architecture/development.md#testing-and-validation).

## Architectural invariants (do not break without an explicit decision)

1. **Landing is raw.** Never dedup, filter or "fix" records before the S3 landing bucket.
   Bronze accepts invalid rows too. Quality gates belong in silver.
2. **At-least-once.** Kafka auto-commit stays off. Commit offsets only after data is durably
   written. Don't introduce code that assumes exactly-once.
3. **Layout contract:** `<prefix>/date=YYYY-MM-DD/symbol=SYM/part-<epoch_ms>-<uuid8>.parquet`,
   with `date` from `event_timestamp` in **UTC**, written via `.tmp` + rename. Spark depends on it.
4. **Kafka key = `symbol`** (per-symbol ordering).
5. **Timestamps are epoch milliseconds** (`long`). `trade_id` is *not* unique in raw data.
6. **The Trade schema has five copies** (Avro, producer dict, consumer `ARROW_SCHEMA`, Spark
   `write_to_iceberg` columns, `validate_dataframe` defaults). Change them together with the
   `change-trade-schema` skill.

## Coding conventions (match existing code)

- Python 3.11 syntax: built-in generics (`list[str]`, `dict[str, int]`), `Optional[...]`,
  `@dataclass` config objects, module docstring plus Google-style `Args:`/`Returns:` docstrings.
- New topics follow the template-method pattern: subclass `BaseProducer`/`BaseConsumer`, put
  them in `src/<topic>.py` with a `main()`, and configure through env vars prefixed by topic
  (`TRADES_*`). See the `add-kafka-topic` skill.
- Configuration comes **only from environment variables** (`os.getenv` with defaults, plus
  `load_dotenv()`). Don't add config files or CLI flags to the producer or consumer.
- Logging: `self.logger = logging.getLogger(self.__class__.__name__)` (classes) or
  `logging.getLogger(__name__)` (modules), with f-string messages. Spark uses
  `utils.setup_logging`. No `print` except in CLI summaries and `__main__` self-tests.
- Errors: the consumer logs and continues (re-buffering on flush failure). Spark helpers
  log and return `bool` or raise, and `main()` returns an exit code. Keep the existing
  per-module style.
- Quote style differs by module: single quotes in `producers/` and `consumers/src/*.py`,
  double quotes in `consumers/src/uploaders/` and `spark/`. Follow the file you're editing.
- Dependencies are per component (`*/requirements.txt`). Don't add packages without asking.
  Keep Spark pins consistent with the JAR versions in `spark/jobs/config.py`.

## Config, secrets and real-AWS safety

- **Never read, print or copy `.env`.** Use `.env.example` (emulator values only) for reference.
- Don't add real bucket names, account IDs or credentials to code or docs. `spark/jobs/config.py`
  already hardcodes one (K22). Don't propagate it.
- **Ask before running anything that can touch real AWS:** `make spark-bronze*` targets other
  than `-floci`, `./spark/run_local.sh` without `--env floci`, `--env local`, and
  `make setup-floci-iceberg` / `verify-floci-iceberg` unless `AWS_ENDPOINT_URL=http://localhost:4566`
  is exported in the same command.
- Ask before `make clean` (it runs `docker system prune -f`), `make clean-data`, or deleting
  `floci-data/` or `warehouse/`.
- There are no DB migrations. Iceberg table changes (partition spec, schema evolution,
  dropping tables) are the equivalent: describe the plan and get confirmation first.

## Definition of done

1. The change follows the invariants and conventions above.
2. You ran the minimum validation for the touched area and reported actual results, including
   anything you couldn't run (for example, the stack wasn't up).
3. Docs updated per the rules below. If you fixed a `known-issues.md` item, remove it.

## Specialist agents and skills

| Use | When |
|---|---|
| `pipeline-reviewer` agent | Review any non-trivial diff for invariant violations, delivery-semantics regressions, schema/layout drift and doc drift. Read-only. |
| `ingestion-engineer` agent | Changes to `producers/`, `consumers/`, Kafka/Floci services in compose, related Make targets |
| `spark-engineer` agent | Changes to `spark/jobs/`, Iceberg tables, medallion layers |
| `run-local-pipeline` skill | Start the stack, run end-to-end smoke tests, diagnose "no data in S3/Iceberg" |
| `change-trade-schema` skill | Any field add/remove/rename/type change |
| `add-kafka-topic` skill | A new topic (schema + producer + consumer + compose + Make) |
| `add-spark-layer-job` skill | Build the silver/gold jobs, or any new Spark job |

## Keeping docs current

- `docs/architecture/` is the single source of truth for architecture. Update the relevant file
  in the same change as the code. Label anything inferred.
- New or changed design decisions go in [decisions.md](docs/architecture/decisions.md). Also add
  a line to `MEMORY.md`'s decisions log, since Copilot reads it.
- Don't duplicate architecture detail into this file, agents or skills. Link instead.
- If you discover new drift or bugs, add them to `known-issues.md` rather than silently working around them.
