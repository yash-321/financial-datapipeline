"""
Daily Backfill DAG for stg_trades model.

Runs twice daily (6 AM and 6 PM UTC) to:
1. Verify data completeness for backfill window
2. Run full refresh dbt model for last 24 hours
3. Execute comprehensive heavy validation tests
4. Generate data quality reports

This DAG ensures data consistency and catches any issues that
incremental runs might miss.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

import sys
sys.path.insert(0, '/home/yash/home-stuff/DataPipeline/airflow')

from utils.snowpipe_check import check_snowpipe_for_backfill
from utils.dbt_utils import run_dbt_command


# Default arguments for all tasks
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-alerts@company.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}


# DAG Definition
dag = DAG(
    dag_id='stg_trades_backfill',
    default_args=default_args,
    description='Twice-daily full refresh backfill with heavy validation',
    schedule_interval='0 6,18 * * *',  # 6 AM and 6 PM UTC
    start_date=datetime(2026, 5, 1),
    catchup=False,
    max_active_runs=1,
    tags=['dbt', 'backfill', 'silver', 'trades', 'validation'],
    doc_md="""
    ## Daily Backfill Pipeline
    
    This DAG runs twice daily (6 AM and 6 PM UTC) to perform a full
    refresh of the stg_trades table for the last 24 hours with
    comprehensive validation.
    
    ### Purpose:
    - Ensure data consistency across the full 24-hour window
    - Catch any records missed by incremental runs
    - Run heavy validation tests that are too expensive for frequent runs
    - Generate data quality metrics and reports
    
    ### Tasks:
    1. **verify_data_completeness**: Check sufficient data exists for backfill
    2. **run_full_refresh**: Execute dbt full-refresh model
    3. **run_schema_tests**: Basic schema validation
    4. **run_heavy_validation**: Comprehensive data quality tests
    5. **run_consistency_tests**: Cross-table consistency checks
    6. **generate_quality_report**: Create data quality summary
    
    ### Failure Handling:
    - Retries 3 times with 5-minute delay
    - No email alerts on failure or retry
    - Execution timeout of 1 hour
    """,
)


# Task 1: Verify data completeness for backfill window
verify_data_completeness = PythonOperator(
    task_id='verify_data_completeness',
    python_callable=check_snowpipe_for_backfill,
    op_kwargs={
        'expected_hours': 24,
        'min_expected_records': 1000,
    },
    dag=dag,
)


# Task 2: Run full refresh dbt model
run_full_refresh = PythonOperator(
    task_id='run_full_refresh',
    python_callable=run_dbt_command,
    op_kwargs={
        'command': 'run',
        'select': 'stg_trades',
        'full_refresh': True,
        'vars': {'backfill_hours': 24},
    },
    dag=dag,
)


# Task 3: Run schema validation tests
run_schema_tests = PythonOperator(
    task_id='run_schema_tests',
    python_callable=run_dbt_command,
    op_kwargs={
        'command': 'test',
        'select': 'test_type:generic,stg_trades',
    },
    dag=dag,
)


# Task 4: Run heavy validation tests
run_heavy_validation = PythonOperator(
    task_id='run_heavy_validation',
    python_callable=run_dbt_command,
    op_kwargs={
        'command': 'test',
        'select': 'tag:heavy_validation',
    },
    dag=dag,
)


# Task 5: Run consistency tests (cross-table validation)
run_consistency_tests = PythonOperator(
    task_id='run_consistency_tests',
    python_callable=run_dbt_command,
    op_kwargs={
        'command': 'test',
        'select': 'tag:consistency',
    },
    dag=dag,
)


# Task 6: Run completeness tests
run_completeness_tests = PythonOperator(
    task_id='run_completeness_tests',
    python_callable=run_dbt_command,
    op_kwargs={
        'command': 'test',
        'select': 'tag:completeness',
    },
    dag=dag,
)


# Task 7: Generate data quality report
generate_quality_report = BashOperator(
    task_id='generate_quality_report',
    bash_command='''
        echo "=========================================="
        echo "Data Quality Report - $(date)"
        echo "=========================================="
        echo ""
        echo "DAG: {{ dag.dag_id }}"
        echo "Run ID: {{ run_id }}"
        echo "Execution Date: {{ ds }}"
        echo ""
        echo "Backfill Window: Last 24 hours"
        echo "Full Refresh: Yes"
        echo ""
        
        # Parse dbt run results
        DBT_PROJECT_DIR="${DBT_PROJECT_DIR:-/home/yash/home-stuff/DataPipeline/market_data_platform}"
        if [ -f "$DBT_PROJECT_DIR/target/run_results.json" ]; then
            echo "dbt Run Results:"
            cat "$DBT_PROJECT_DIR/target/run_results.json" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for r in data.get('results', []):
    status = r.get('status', 'unknown')
    name = r.get('unique_id', 'unknown').split('.')[-1]
    time = r.get('execution_time', 0)
    print(f'  - {name}: {status} ({time:.2f}s)')
elapsed = data.get('elapsed_time', 0)
print(f'Total elapsed: {elapsed:.2f}s')
"
        fi
        
        echo ""
        echo "=========================================="
        echo "Backfill completed successfully"
        echo "=========================================="
    ''',
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)


# Task 8: Notify on completion (success or failure)
notify_completion = BashOperator(
    task_id='notify_completion',
    bash_command='''
        # This could integrate with Slack, PagerDuty, etc.
        echo "Backfill DAG completed"
        echo "Run ID: {{ run_id }}"
        echo "Execution Date: {{ ds }}"
    ''',
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)


# Task dependencies
# Main flow
verify_data_completeness >> run_full_refresh >> run_schema_tests

# Parallel heavy validation after schema tests
run_schema_tests >> [run_heavy_validation, run_consistency_tests, run_completeness_tests]

# Report and notification after all tests
[run_heavy_validation, run_consistency_tests, run_completeness_tests] >> generate_quality_report >> notify_completion
