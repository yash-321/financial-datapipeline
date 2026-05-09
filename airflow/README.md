# Airflow Setup for Market Data Pipeline

This directory contains Airflow DAGs and utilities for orchestrating the dbt data pipeline.

## DAGs

### 1. `stg_trades_incremental` (Every 3 minutes)
Frequent incremental updates to the `stg_trades` silver table.

**Schedule:** `*/3 * * * *` (every 3 minutes)

**Tasks:**
1. `check_snowpipe` - Verify Snowpipe health and data freshness
2. `run_incremental_model` - Execute dbt incremental model
3. `run_schema_tests` - Basic schema validation (not_null, unique)
4. `run_data_tests` - Incremental-specific validation tests
5. `log_metrics` - Log completion metrics

**Tests Run:**
- Schema tests (not_null, unique on trade_id, symbol, timestamps)
- `test_no_duplicates` (incremental_validation)
- `test_data_freshness` (incremental_validation)

### 2. `stg_trades_backfill` (Twice daily)
Full refresh with comprehensive validation for the last 24 hours.

**Schedule:** `0 6,18 * * *` (6 AM and 6 PM UTC)

**Tasks:**
1. `verify_data_completeness` - Ensure sufficient data for backfill
2. `run_full_refresh` - Execute dbt full-refresh model
3. `run_schema_tests` - Basic schema validation
4. `run_heavy_validation` - Comprehensive data quality tests
5. `run_consistency_tests` - Cross-table consistency checks
6. `run_completeness_tests` - Data completeness validation
7. `generate_quality_report` - Summary report
8. `notify_completion` - Completion notification

**Heavy Validation Tests:**
- `test_row_count_consistency` - Bronze vs Silver row counts
- `test_symbol_completeness` - All expected symbols present
- `test_time_series_continuity` - No gaps in hourly data
- `test_price_anomalies` - Price bounds and volatility checks
- `test_quantity_validation` - Valid trade quantities
- `test_timestamp_consistency` - Logical timestamp ordering

## Setup

### Prerequisites
```bash
pip install -r requirements.txt
```

### Environment Variables
Set these in your Airflow environment or `.env` file:

```bash
# Snowflake Connection
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=DATAPIPELINE_WH
SNOWFLAKE_DATABASE=DATAPIPELINE
SNOWFLAKE_SCHEMA=TRADES
SNOWFLAKE_ROLE=SYSADMIN

# dbt Configuration
DBT_PROJECT_DIR=/path/to/market_data_platform
DBT_PROFILES_DIR=~/.dbt
```

### Airflow Connection (Alternative)
Create a Snowflake connection in Airflow with ID `snowflake_default`:

```python
from airflow.models import Connection

conn = Connection(
    conn_id='snowflake_default',
    conn_type='snowflake',
    login='your_user',
    password='your_password',
    extra={
        'account': 'your_account',
        'warehouse': 'DATAPIPELINE_WH',
        'database': 'DATAPIPELINE',
        'schema': 'TRADES',
        'role': 'SYSADMIN'
    }
)
```

## Directory Structure

```
airflow/
├── dags/
│   ├── __init__.py
│   ├── stg_trades_incremental.py  # Frequent incremental DAG
│   └── stg_trades_backfill.py     # Twice-daily backfill DAG
├── plugins/
│   └── __init__.py
├── utils/
│   ├── __init__.py
│   ├── snowpipe_check.py          # Snowpipe health utilities
│   └── dbt_utils.py               # dbt command wrappers
├── requirements.txt
└── README.md
```

## dbt Tests by Tag

| Tag | Description | Used By |
|-----|-------------|---------|
| `incremental_validation` | Fast tests for incremental runs | Incremental DAG |
| `heavy_validation` | Comprehensive tests (expensive) | Backfill DAG |
| `consistency` | Cross-table consistency checks | Backfill DAG |
| `completeness` | Data completeness validation | Backfill DAG |

## Monitoring

### Logs
DAG run logs are available in the Airflow UI or at:
- Airflow logs: `$AIRFLOW_HOME/logs/`
- dbt logs: `market_data_platform/logs/`

### Alerts
Configure email alerts in `default_args`:
```python
default_args = {
    'email': ['your-team@company.com'],
    'email_on_failure': True,
    'email_on_retry': True,
}
```

### Metrics
Key metrics tracked:
- Snowpipe ingestion rate and errors
- dbt model execution time
- Test pass/fail rates
- Data staleness

## Troubleshooting

### Snowpipe Issues
If `check_snowpipe` fails:
1. Check Snowpipe status: `SELECT SYSTEM$PIPE_STATUS('trades_pipe')`
2. Review copy history: `SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(...))`
3. Verify S3 event notifications are configured

### dbt Test Failures
1. Check the specific test output in Airflow logs
2. Query the compiled test SQL in `target/compiled/`
3. Run manually: `dbt test --select <test_name>`

### Data Freshness Issues
If `test_data_freshness` fails:
1. Verify Kafka producer is running
2. Check consumer lag in Kafka
3. Verify Snowpipe is processing files
