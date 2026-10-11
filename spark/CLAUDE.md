# spark/: Spark + Iceberg specifics

Supplements the root `CLAUDE.md`. Flow and environment matrix:
[docs/architecture/data-flows.md §2](../docs/architecture/data-flows.md#2-batch-s3-landing--iceberg-spark).

## Layout

- `jobs/config.py`: dataclasses (`S3Config`, `IcebergConfig`, `SparkConfig`, `PipelineConfig`),
  `LAYER_CONFIG` (layer → Glue DB/table), and `get_config(env, layer)`. Loads `<repo>/.env`.
- `jobs/utils.py`: reusable steps: `create_spark_session`, `setup_logging`, `read_landing_data`,
  `validate_dataframe`, `write_to_iceberg`, `verify_write`.
- `jobs/landing_to_bronze.py`: CLI (`--env local|floci`, `--layer`, `--mode`, `--dry-run`,
  `--log-level`). `main()` is a numbered sequence of steps and returns an exit code.
- `landing_to_bronze.ipynb`, `setup_guide.md`, `troubleshooting_guide.md`, `glue_catalog_setup.md`:
  learning material written for the notebook. Its catalog and warehouse differ from the job's.
  Don't treat it as the spec.

## Rules that differ from the rest of the repo

- **Flat imports.** Modules import each other as `from config import …` / `from utils import …`.
  Jobs must run with cwd `spark/jobs` (Make targets `cd` there). Keep this style for new jobs
  in the same directory. Don't convert to package imports piecemeal.
- **New jobs reuse `utils.py`** helpers and follow `landing_to_bronze.py`'s numbered-step structure. Wrap **every**
  step in try/except with `spark.stop()` and `return 1` (the existing job only wraps steps 2–4;
  a failure in validate/write/verify escapes as a traceback).
  Use `--dry-run` support, and keep `main() -> int` with `sys.exit(main())`.
- **Environment selection lives only in `get_config`.** Don't read env vars or hardcode
  endpoints elsewhere.
- **JAR and Python versions are coupled:** `pyspark==3.5.0` ↔ `iceberg-spark-runtime-3.5_2.12`,
  `hadoop-aws:3.3.4` ↔ `aws-java-sdk-bundle:1.12.262`, and Iceberg runtime ↔ `iceberg-aws-bundle`
  (both `1.4.1`). Bump them together.
- **Iceberg table changes are migrations.** Changing partition specs, dropping or recreating
  tables, or rewriting with `overwrite` on a populated table needs explicit user confirmation.
  `--mode overwrite` replaces the whole table.

## Running safely

- Default to `--env floci`. `--env local` (including `make spark-bronze`, `spark-bronze-dry` and
  `./run_local.sh` defaults) reads **real AWS S3** with `.env` credentials. Ask first.
- Floci prerequisites: the stack is up, `AWS_ENDPOINT_URL=http://localhost:4566` is exported,
  `make setup-floci-iceberg` has run, and landing data exists (`make list-objects`).
- Cheap checks: `make spark-test-config` (no JVM), and `python -m py_compile jobs/*.py`.
  Full check: `python landing_to_bronze.py --env floci --dry-run`, then `make spark-bronze-floci`
  and `make verify-floci-iceberg`.
- The first Spark run downloads JARs from Maven (needs network, takes a while).

## Current gaps to keep in mind

`--mode append` is not incremental (K2). Tables created by the job are unpartitioned (K3).
`--layer silver|gold` writes raw rows (K4). See [known-issues.md](../docs/architecture/known-issues.md).
Use the `add-spark-layer-job` skill when building silver or gold.
