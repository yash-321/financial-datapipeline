"""
dbt utilities for Airflow.

Provides functions to run dbt commands with proper error handling
and result parsing for Airflow integration.
"""

import os
import json
import subprocess
import logging
from pathlib import Path
from typing import Optional

from airflow.exceptions import AirflowFailException

logger = logging.getLogger(__name__)

# Default paths - adjust based on your deployment
DBT_PROJECT_DIR = os.environ.get(
    'DBT_PROJECT_DIR',
    '/home/yash/home-stuff/DataPipeline/market_data_platform'
)
DBT_PROFILES_DIR = os.environ.get(
    'DBT_PROFILES_DIR',
    os.path.expanduser('~/.dbt')
)


def run_dbt_command(
    command: str,
    select: Optional[str] = None,
    exclude: Optional[str] = None,
    full_refresh: bool = False,
    vars: Optional[dict] = None,
    fail_fast: bool = True,
    project_dir: Optional[str] = None,
    profiles_dir: Optional[str] = None,
    target: Optional[str] = None,
    **context
) -> dict:
    """
    Execute a dbt command and return results.
    
    Args:
        command: dbt command (run, test, build, etc.)
        select: Model selection syntax
        exclude: Models to exclude
        full_refresh: Run with --full-refresh flag
        vars: Variables to pass to dbt
        fail_fast: Stop on first failure
        project_dir: Path to dbt project
        profiles_dir: Path to profiles directory
        target: dbt target to use
        context: Airflow context
    
    Returns:
        dict with command results
    
    Raises:
        AirflowFailException: If dbt command fails
    """
    project_dir = project_dir or DBT_PROJECT_DIR
    profiles_dir = profiles_dir or DBT_PROFILES_DIR
    
    cmd = ['dbt', command]
    
    if select:
        cmd.extend(['--select', select])
    
    if exclude:
        cmd.extend(['--exclude', exclude])
    
    if full_refresh:
        cmd.append('--full-refresh')
    
    if vars:
        cmd.extend(['--vars', json.dumps(vars)])
    
    if fail_fast:
        cmd.append('--fail-fast')
    
    if target:
        cmd.extend(['--target', target])
    
    cmd.extend(['--project-dir', project_dir])
    cmd.extend(['--profiles-dir', profiles_dir])
    
    logger.info(f"Running dbt command: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=project_dir,
            timeout=1800  # 30 minute timeout
        )
        
        output = {
            'command': ' '.join(cmd),
            'returncode': result.returncode,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'success': result.returncode == 0
        }
        
        # Parse run results if available
        run_results_path = Path(project_dir) / 'target' / 'run_results.json'
        if run_results_path.exists():
            with open(run_results_path) as f:
                run_results = json.load(f)
                output['run_results'] = {
                    'elapsed_time': run_results.get('elapsed_time'),
                    'results': [
                        {
                            'unique_id': r.get('unique_id'),
                            'status': r.get('status'),
                            'execution_time': r.get('execution_time'),
                            'rows_affected': r.get('adapter_response', {}).get('rows_affected')
                        }
                        for r in run_results.get('results', [])
                    ]
                }
        
        if context.get('ti'):
            context['ti'].xcom_push(key=f'dbt_{command}_result', value=output)
        
        if result.returncode != 0:
            logger.error(f"dbt {command} failed:\n{result.stderr}\n{result.stdout}")
            raise AirflowFailException(f"dbt {command} failed with return code {result.returncode}")
        
        logger.info(f"dbt {command} completed successfully")
        return output
        
    except subprocess.TimeoutExpired:
        raise AirflowFailException(f"dbt {command} timed out after 30 minutes")


def run_dbt_incremental(
    model: str = 'stg_trades',
    run_tests: bool = True,
    test_select: Optional[str] = None,
    **context
) -> dict:
    """
    Run an incremental dbt model with optional tests.
    
    Args:
        model: Model to run incrementally
        run_tests: Whether to run tests after model
        test_select: Specific tests to run (defaults to model tests)
        context: Airflow context
    
    Returns:
        dict with combined results
    """
    results = {}
    
    # Run the incremental model
    results['model'] = run_dbt_command(
        command='run',
        select=model,
        full_refresh=False,
        **context
    )
    
    # Run tests if requested
    if run_tests:
        test_selector = test_select or f"{model},tag:incremental"
        results['tests'] = run_dbt_command(
            command='test',
            select=test_selector,
            **context
        )
    
    return results


def run_dbt_backfill(
    model: str = 'stg_trades',
    run_heavy_tests: bool = True,
    hours_back: int = 24,
    **context
) -> dict:
    """
    Run a full refresh backfill with heavy validation.
    
    Args:
        model: Model to backfill
        run_heavy_tests: Whether to run heavy validation tests
        hours_back: Hours of data to backfill
        context: Airflow context
    
    Returns:
        dict with combined results
    """
    results = {}
    
    # Run full refresh
    results['model'] = run_dbt_command(
        command='run',
        select=model,
        full_refresh=True,
        vars={'backfill_hours': hours_back},
        **context
    )
    
    # Run schema tests
    results['schema_tests'] = run_dbt_command(
        command='test',
        select=model,
        **context
    )
    
    # Run heavy validation tests
    if run_heavy_tests:
        results['heavy_tests'] = run_dbt_command(
            command='test',
            select='tag:heavy_validation',
            **context
        )
    
    return results


def get_dbt_manifest() -> dict:
    """Load and return the dbt manifest."""
    manifest_path = Path(DBT_PROJECT_DIR) / 'target' / 'manifest.json'
    if manifest_path.exists():
        with open(manifest_path) as f:
            return json.load(f)
    return {}


def get_model_freshness(**context) -> dict:
    """
    Check source freshness using dbt.
    
    Returns:
        dict with freshness results
    """
    return run_dbt_command(
        command='source',
        select='freshness',
        **context
    )
