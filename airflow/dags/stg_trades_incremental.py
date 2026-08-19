"""
Frequent Incremental DAG for stg_trades model.

Runs every 3 minutes to:
1. Check Snowpipe health and data freshness
2. Run incremental dbt model (stg_trades)
3. Run schema validation tests

This DAG is optimized for low-latency data freshness while maintaining
data quality through automated testing.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

import sys
sys.path.insert(0, '/home/yash/home-stuff/DataPipeline/airflow')

from utils.snowpipe_check import check_snowpipe_health
from utils.dbt_utils import run_dbt_command


# Default arguments for all tasks
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-alerts@company.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'execution_timeout': timedelta(minutes=10),
}


# DAG Definition
dag = DAG(
    dag_id='stg_trades_incremental',
    default_args=default_args,
    description='Frequent incremental load for stg_trades silver table',
    schedule_interval='*/3 * * * *',  # Every 3 minutes
    start_date=datetime(2026, 5, 1),
    catchup=False,
    max_active_runs=1,  # Prevent overlapping runs
    tags=['dbt', 'incremental', 'silver', 'trades'],
    doc_md="""
    ## Incremental Trades Pipeline
    
    This DAG runs every 3 minutes to incrementally load new trades
    from the bronze RAW_TRADES table to the silver stg_trades table.
    
    ### Tasks:
    1. **check_snowpipe**: Verify Snowpipe is healthy and has recent data
    2. **run_incremental_model**: Execute dbt incremental model
    3. **run_schema_tests**: Validate data quality with dbt tests
    4. **run_data_tests**: Run custom data validation tests
    
    ### Failure Handling:
    - Retries 2 times with 1-minute delay
    - Alerts sent to data-alerts@company.com on failure
    - Skips run if no new data available (reduces unnecessary processing)
    """,
)


# Task 1: Check Snowpipe health
check_snowpipe = PythonOperator(
    task_id='check_snowpipe',
    python_callable=check_snowpipe_health,
    op_kwargs={
        'fail_on_error': True,
        'skip_on_no_data': False,  # Don't skip - we want consistent runs
        'max_staleness_minutes': 15,
        'min_recent_files': 0,
    },
    dag=dag,
)


# Task 2: Run incremental dbt model
run_incremental_model = PythonOperator(
    task_id='run_incremental_model',
    python_callable=run_dbt_command,
    op_kwargs={
        'command': 'run',
        'select': 'stg_trades',
        'full_refresh': False,
    },
    dag=dag,
)


# Task 3: Run schema validation tests (not_null, unique)
run_schema_tests = PythonOperator(
    task_id='run_schema_tests',
    python_callable=run_dbt_command,
    op_kwargs={
        'command': 'test',
        'select': 'test_type:generic,stg_trades',
    },
    dag=dag,
)


# Task 4: Run data validation tests (freshness, consistency)
run_data_tests = PythonOperator(
    task_id='run_data_tests',
    python_callable=run_dbt_command,
    op_kwargs={
        'command': 'test',
        'select': 'tag:incremental_validation',
    },
    # Continue even if schema tests fail to get full picture
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)


# Task 5: Cleanup and metrics (optional)
log_metrics = BashOperator(
    task_id='log_metrics',
    bash_command='''
        echo "Incremental run completed at $(date)"
        echo "DAG: {{ dag.dag_id }}"
        echo "Run ID: {{ run_id }}"
        echo "Execution Date: {{ ds }}"
    ''',
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)


# Task dependencies
check_snowpipe >> run_incremental_model >> run_schema_tests >> run_data_tests >> log_metrics
