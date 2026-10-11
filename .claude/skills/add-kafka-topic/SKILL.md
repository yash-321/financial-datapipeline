---
name: add-kafka-topic
description: Add a new Kafka topic end to end, covering the Avro schema, a BaseProducer subclass, a BaseConsumer subclass that writes Hive-partitioned Parquet to its own S3 prefix, the docker-compose service, topic creation in init-kafka, Make targets and docs. Use when asked to ingest a new entity (for example quotes, accounts or orders) alongside trades.
---

# Add a new Kafka topic

The README's "Adding New Topics" section is a sketch. This skill lists the extra places that
must change and the env-var collisions that would silently mix data. The examples below use
the topic name `quotes`. Substitute your own.

## 1. Schema

Create `configs/schemas/quote_schema.avsc` (namespace `com.datapipeline.<topic>`, a `doc` on
every field, timestamps as `long` epoch **ms**). Include an event-time field. The consumer
partitions by it.

## 2. Producer: `producers/src/quotes.py`

Follow `producers/src/trades.py`:

- `class QuotesProducer(BaseProducer)` implementing `generate()` and `get_key()`. The key
  decides partition ordering. Use the entity whose order matters.
- Override `transform_record()` only for send-time fields.
- `main()` reads env vars **explicitly with a topic prefix**, the way `trades.main()` does:
  `QUOTES_TOPIC`, `QUOTES_SCHEMA_PATH` (default `configs/schemas/quote_schema.avsc`, relative
  to the repo root), plus shared `KAFKA_BOOTSTRAP_SERVERS`. Don't use
  `ProducerConfig.from_env`: its `<P>_KAFKA_TOPIC` naming differs from the `TRADES_TOPIC`
  convention, and it is unused.
- Run as `python -m producers.src.quotes` from the repo root.

## 3. Consumer: `consumers/src/quotes.py`

Follow `consumers/src/trades.py`:

- `class QuotesConsumer(BaseConsumer)` with an `ARROW_SCHEMA` matching the Avro field order
  and types.
- `get_partition_keys()` returns Hive-style strings, for example `(f"date={…}", f"symbol={…}")`,
  with the date taken from the **event** timestamp in UTC. Keep cardinality low. The README
  example `account_id=…` would create one directory and file per account per flush.
- `process_batch()`: reuse the trades implementation (`.tmp` + rename, Snappy,
  `row_group_size=10_000`, queue the upload with a sync fallback). It differs only in
  `ARROW_SCHEMA`. Copying it is consistent with today's code. Moving it into `BaseConsumer`
  is a refactor, so propose it to the user rather than doing it silently.
- Override `stop()` to `wait_for_uploads()` the way `TradesConsumer` does.
- `main()` reads `QUOTES_TOPIC`, `QUOTES_GROUP_ID`, `QUOTES_SCHEMA_PATH` and `QUOTES_OUTPUT_DIR`,
  plus the shared `S3_*`/`AWS_*` vars, and calls `setup_signal_handlers`.

## 4. docker-compose.yml

Add a service modelled on `trade-consumer`:

```yaml
  quote-consumer:
    profiles: [consumers]
    build:
      context: ./consumers
      dockerfile: Dockerfile
      args:
        CONSUMER_MODULE: quotes          # Dockerfile runs python -m src.${CONSUMER_MODULE}
    container_name: quote-consumer
    env_file: [.env]
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      QUOTES_TOPIC: quotes_topic
      QUOTES_GROUP_ID: quote_consumer_group     # must differ from trade_consumer_group
      QUOTES_SCHEMA_PATH: /app/configs/schemas/quote_schema.avsc
      QUOTES_OUTPUT_DIR: /app/data/quotes
      S3_BUCKET: ${S3_BUCKET:-}
      S3_PREFIX: quotes                          # REQUIRED: .env sets S3_PREFIX=trades
      AWS_ENDPOINT_URL: http://floci:4566
      # copy AWS_REGION / credentials / DELETE_LOCAL_AFTER_UPLOAD / FLUSH_INTERVAL_SECONDS from trade-consumer
    volumes:
      - ./configs/schemas:/app/configs/schemas:ro
      - ./data/quotes:/app/data/quotes
    networks: [pipeline]
    depends_on:
      kafka: {condition: service_healthy}
      floci: {condition: service_healthy}
    restart: unless-stopped
```

**Pitfall:** `env_file: .env` injects `S3_PREFIX=trades`. If you don't override it under
`environment:`, the new consumer writes into `trades/`, and Spark's landing read
(`s3a://…/trades/`) would ingest the wrong schema.

Add the topic to the `init-kafka` command:
`kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic quotes_topic --partitions 3 --replication-factor 1`.

## 5. Makefile, .env.example, .gitignore

- Add `logs-quote-consumer`, and optionally `produce-quotes`
  (`python -m producers.src.quotes`, run from the repo root, not `cd producers`). Add them to
  `help` and `.PHONY`.
- `.env.example`: add `QUOTES_TOPIC`, `QUOTES_GROUP_ID` and `QUOTES_SCHEMA_PATH` with
  non-secret defaults.
- `.gitignore`: add `data/quotes/`. (`*.parquet` is already ignored globally.)

## 6. Validate

```bash
python -m py_compile producers/src/quotes.py consumers/src/quotes.py
docker compose --profile consumers config -q
docker compose --profile consumers up -d --build quote-consumer
docker compose up init-kafka            # re-run topic creation, or rely on auto-create
TRADE_COUNT=200 python -m producers.src.quotes   # or your count env var
docker logs -f quote-consumer           # Processed / Committed offsets / Uploaded
make list-objects                        # expect quotes/date=…/… alongside trades/
```

Then run the `pipeline-reviewer` agent on the diff.

## 7. Docs

- `docs/architecture/overview.md`: component table and diagram.
- `docs/architecture/data-model.md`: new schema, layout, Kafka contract and env vars.
- `docs/architecture/data-flows.md`, if the flow differs from trades.
- `README.md`: services table and project structure.
- Spark: nothing reads the new prefix until a job is added (see the `add-spark-layer-job`
  skill). `spark/jobs/config.py` hardcodes `trades/` in `landing_path`.
