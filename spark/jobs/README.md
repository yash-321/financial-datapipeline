# Spark Jobs: Landing to Bronze

Production Spark jobs for the data pipeline.

## 📁 File Structure

```
spark/jobs/
├── __init__.py           # Package marker
├── config.py             # Configuration classes (S3, Iceberg, Spark)
├── utils.py              # Reusable utilities (session, validation, logging)
├── landing_to_bronze.py  # Main job (CLI entry point)
└── README.md             # This file
```

## 🚀 Quick Start

### Run the Job

```bash
# Activate environment
conda activate datapipeline

# Full refresh (overwrite mode)
python spark/jobs/landing_to_bronze.py --env local

# Incremental load
python spark/jobs/landing_to_bronze.py --env local --mode append

# Dry run (validate only)
python spark/jobs/landing_to_bronze.py --env local --dry-run

# With floci (LocalStack)
python spark/jobs/landing_to_bronze.py --env floci
```

Or use the shell script:
```bash
./spark/run_local.sh
./spark/run_local.sh --dry-run
./spark/run_local.sh --env floci
```

### Configuration

The job reads AWS credentials from `.env` file in project root:
```
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_REGION=eu-west-1
S3_BUCKET=financial-data-landing-587129419094-eu-west-1-an
```

### Test Configuration

```bash
cd spark/jobs
python config.py  # Prints config for both environments
```

## 📖 Module Reference

### config.py

Configuration dataclasses and factory function:
- `S3Config` - S3 bucket, region, endpoint, credentials
- `IcebergConfig` - Catalog, database, table, warehouse path
- `SparkConfig` - App name, memory, JAR packages
- `PipelineConfig` - Combines all configs
- `get_config(env)` - Factory for "local" or "floci" environments

### utils.py

Reusable utility functions:
- `create_spark_session(config)` - Build SparkSession with Iceberg + S3
- `setup_logging(name, level)` - Configure structured logging
- `read_landing_data(spark, path, logger)` - Read Parquet from S3
- `validate_dataframe(df, columns, logger)` - Check data quality
- `write_to_iceberg(df, table, mode, logger)` - Write to Iceberg table
- `verify_write(spark, df, table, logger)` - Verify counts match

### landing_to_bronze.py

Main CLI entry point:
- `parse_args()` - Parse --env, --mode, --dry-run, --log-level
- `main()` - Orchestrate the full ETL pipeline

### Step 3: Implement the Main Job (`landing_to_bronze.py`)

Open `landing_to_bronze.py` and complete the TODOs:

1. **parse_args()** - CLI argument parsing
2. **main()** - Orchestrate the pipeline

### Step 4: Run Locally

```bash
# Make the script executable
chmod +x ../run_local.sh

# Run with local environment
../run_local.sh --env local

# Or run Python directly
python landing_to_bronze.py --env local
```

### Step 5: Run with Floci

```bash
# Start floci (from project root)
cd ../..
make up-floci

# Create landing bucket in floci
make create-bucket

# Upload test data to floci (you'll need to implement this)
# aws --endpoint-url=http://localhost:4566 s3 cp ...

# Run job with floci
cd spark/jobs
python landing_to_bronze.py --env floci
```

## 📋 Command Line Options

```
python landing_to_bronze.py [OPTIONS]

Options:
  --env {local,floci}     Environment to run in (default: local)
  --mode {overwrite,append}  Write mode (default: overwrite)
  --dry-run               Validate but don't write
  --log-level {DEBUG,INFO,WARNING,ERROR}  Logging level (default: INFO)
```

## 🔧 Configuration Reference

### Local Environment

- **S3**: Uses real AWS S3 (credentials from `.env`)
- **Iceberg Catalog**: Local Hadoop catalog
- **Warehouse**: `spark/warehouse/` (local filesystem)

### Floci Environment

- **S3**: Uses floci LocalStack emulator (localhost:4566)
- **Iceberg Catalog**: Local Hadoop catalog (or Glue if configured)
- **Warehouse**: Can be local or in floci S3

### Environment Variables (`.env`)

```env
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_REGION=eu-west-1
S3_BUCKET=financial-data-landing-587129419094-eu-west-1-an
```

## 🧪 Testing

### Unit Tests

```bash
# TODO: Add pytest tests
pytest tests/test_config.py
pytest tests/test_utils.py
```

### Integration Tests

```bash
# Run with dry-run to validate without writing
python landing_to_bronze.py --env local --dry-run

# Check Iceberg table exists
# (Use spark-sql or your notebook)
```

## 🔍 Troubleshooting

### "No module named 'config'"

Run from the jobs directory:
```bash
cd spark/jobs
python landing_to_bronze.py
```

### "S3 access denied"

Check your `.env` file has valid AWS credentials.

### "Iceberg table not found"

The job creates the table if it doesn't exist (with mode=overwrite).
For append mode, run overwrite first.

### "Java not found"

Install Java 8 or 11:
```bash
# Ubuntu/Debian
sudo apt install openjdk-11-jdk

# macOS
brew install openjdk@11
```

### "Out of memory"

Increase driver memory in `config.py`:
```python
driver_memory: str = "8g"  # Increase from 4g
```

## 📚 Next Steps

1. **Implement incremental processing** - Track watermarks in a config table
2. **Add Silver layer job** - Deduplication and enrichment
3. **Add tests** - Unit tests for config and utils
4. **Add monitoring** - Metrics and alerting
5. **Add Airflow DAG** - Schedule the job

## 📖 Reference

- [Your notebook](../landing_to_bronze.ipynb) - Interactive version of this job
- [Setup guide](../setup_guide.md) - Environment setup
- [Troubleshooting](../troubleshooting_guide.md) - Common issues
