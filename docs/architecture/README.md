# Architecture Documentation

These documents describe the system **as implemented**. Each one notes where it is
inferring rather than reporting verified behaviour.

| Document | Read it when you need… |
|---|---|
| [overview.md](overview.md) | What the system is, what's built vs planned, component map, boundaries |
| [data-flows.md](data-flows.md) | The end-to-end ingestion and Spark flows, delivery semantics, failure paths |
| [data-model.md](data-model.md) | The Trade schema and its copies, storage layouts, Iceberg tables, Kafka and env-var contracts |
| [decisions.md](decisions.md) | Why things are the way they are (stated vs inferred), open questions |
| [development.md](development.md) | Local setup, start-up order, commands, validation, real-AWS safety |
| [known-issues.md](known-issues.md) | Verified bugs, tech debt, and where other docs are wrong |

Other sources:

- `MEMORY.md`: the author's running project notes, used by Copilot. Useful context, but
  see the drift list in [known-issues.md](known-issues.md#documentation-drift).
- `spark/*.md`: notebook-oriented Spark/Iceberg setup and troubleshooting guides.
- [`../archive/snowflake-era-architecture.md`](../archive/snowflake-era-architecture.md):
  the superseded Snowflake/dbt/Airflow design.
