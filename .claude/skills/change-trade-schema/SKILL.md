---
name: change-trade-schema
description: Safely add, remove, rename or retype a field in the Trade record. The record is defined in configs/schemas/trade_schema.avsc and restated in the producer, the consumer's PyArrow schema and the Spark job. Use whenever a change touches trade fields, the Avro schema, ARROW_SCHEMA, or the column lists in spark/jobs/utils.py. Covers schemaless-Avro rollout, mixed-schema landing files and Iceberg schema evolution.
---

# Change the Trade schema

The Trade shape lives in **five places** with no registry or automated check
(see `docs/architecture/data-model.md`). Messages are **Avro schemaless**: the consumer decodes
with exactly one schema and no writer-schema ID. That makes every schema change, even an
"additive" one, a coordinated producer-and-consumer rollout.

## 1. Classify the change

| Change | Risk | Extra steps |
|---|---|---|
| Add a field | Medium | Drain the topic before rollout. Old landing files lack the column (step 5). |
| Remove a field | High | Same as add, plus check whether Spark, silver or gold use it. |
| Rename a field | High | Treat as remove + add. Old landing and bronze data keep the old name. |
| Change a type or unit (for example, ms → µs, double → decimal) | High | Breaks Parquet ↔ Iceberg compatibility. Needs a migration plan and user sign-off. |
| Change `trade_id`, `symbol` or `event_timestamp` semantics | Critical | These drive the Kafka key, partition layout and dedup key. Stop and confirm with the user. |

## 2. Edit all five copies

1. `configs/schemas/trade_schema.avsc`: add the field with a `doc`. Use a union with
   `"null"` and a `default` if the producer may omit it.
2. `producers/src/trades.py` `TradesProducer.generate()`: populate it. Check
   `send_with_test_events()` copies (`record.copy()`) still make sense for the field.
3. `consumers/src/trades.py` `TradesConsumer.ARROW_SCHEMA`: add it **in the same position**
   as in Avro. Type mapping: string→`pa.string()`, double→`pa.float64()`, long→`pa.int64()`,
   int→`pa.int32()`, boolean→`pa.bool_()`.
4. `spark/jobs/utils.py` `write_to_iceberg()`: add it to the `columns` list (before `date`).
5. `spark/jobs/utils.py` `validate_dataframe()`: add it to the default `required_columns`
   only if every row must have it (old landing files won't).

Then run the static check from the repo root:

```bash
python3 .claude/skills/change-trade-schema/check_schema_alignment.py
```

Exit code 0 means aligned. 1 lists the mismatches. 2 means the code moved and the script
needs updating.

## 3. Roll out on the running stack (schemaless Avro)

Old messages can't be decoded with a new schema. A failure is logged as
`Failed to process message` and the message is skipped, which is data loss (K6).

1. Stop producing. Let the consumer catch up: watch `make logs-consumer` until a
   `Committed offsets` line follows the last produce, or check consumer-group lag in Kafka UI
   (http://localhost:8080).
2. Rebuild and restart the consumer:
   `docker compose --profile consumers up -d --build trade-consumer`.
3. Run the producer with a small batch:
   `TRADE_COUNT=200 python -m producers.src.trades` (repo root).
4. Check the logs for `Failed to process message`. There should be none.

Resetting the topic or consumer group instead of draining is destructive. Ask first.

## 4. Verify landing output

```bash
make list-objects
python scripts/check_parquet.py   # schema printout should include the new column
```

## 5. Spark / Iceberg

- **Mixed landing files.** Older Parquet files lack the field. `spark.read.parquet` without
  `mergeSchema` may pick the schema from a single file and drop or null the column
  unpredictably. Before relying on the field, add `.option("mergeSchema", "true")` in
  `read_landing_data` or pass an explicit schema (this changes `spark/jobs/utils.py`; the
  `spark-engineer` agent owns it).
- **Existing Iceberg tables** won't accept a new column on `append`. Choose one of the
  following, then **confirm with the user before running**:
  - `ALTER TABLE <table> ADD COLUMN <name> <type>` (preserves history), or
  - a full refresh with `--mode overwrite` (rewrites bronze from landing; acceptable today,
    because bronze is a full copy of landing).
- Validate with `cd spark/jobs && python landing_to_bronze.py --env floci --dry-run`, then a
  Floci run. Never use `--env local` without permission (it reads real AWS).

## 6. Update docs

- `docs/architecture/data-model.md`: fields table, plus the schema-copies table if a location changed.
- `docs/architecture/decisions.md` if the change reflects a design decision, plus a line in
  `MEMORY.md`'s decisions log.
- `README.md` only if it shows the schema.

## Common failure modes

| Symptom | Cause |
|---|---|
| `Failed to process message` floods after deploy | Old messages still in the topic (drain first), or producer and consumer schemas differ |
| `pa.Table.from_pylist` error, or nulls in the new column | `ARROW_SCHEMA` not updated, or the field is missing from `generate()` |
| New column absent in bronze | Not added to the `write_to_iceberg` `columns` list, or the landing read without `mergeSchema` |
| `Cannot write incompatible dataset to table` | Iceberg table schema not evolved |
