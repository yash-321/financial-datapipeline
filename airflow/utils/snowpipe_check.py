"""
Snowpipe health check utilities for Airflow.

Provides functions to verify Snowpipe is running and ingesting data
before triggering downstream dbt transformations.
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Optional

import snowflake.connector
from airflow.exceptions import AirflowSkipException, AirflowFailException

logger = logging.getLogger(__name__)


def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    """Create Snowflake connection from Airflow connection or env vars."""
    try:
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection('snowflake_default')
        return snowflake.connector.connect(
            account=conn.extra_dejson.get('account', os.environ.get("SNOWFLAKE_ACCOUNT")),
            user=conn.login or os.environ.get("SNOWFLAKE_USER"),
            password=conn.password or os.environ.get("SNOWFLAKE_PASSWORD"),
            warehouse=conn.extra_dejson.get('warehouse', os.environ.get("SNOWFLAKE_WAREHOUSE", "DATAPIPELINE_WH")),
            database=conn.extra_dejson.get('database', os.environ.get("SNOWFLAKE_DATABASE", "DATAPIPELINE")),
            schema=conn.extra_dejson.get('schema', os.environ.get("SNOWFLAKE_SCHEMA", "TRADES")),
            role=conn.extra_dejson.get('role', os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN")),
        )
    except Exception:
        # Fallback to environment variables
        return snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "DATAPIPELINE_WH"),
            database=os.environ.get("SNOWFLAKE_DATABASE", "DATAPIPELINE"),
            schema=os.environ.get("SNOWFLAKE_SCHEMA", "TRADES"),
            role=os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
        )


def get_pipe_status(conn: snowflake.connector.SnowflakeConnection) -> dict:
    """Get current Snowpipe status."""
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT SYSTEM$PIPE_STATUS('trades_pipe')")
        result = cursor.fetchone()
        if result:
            return json.loads(result[0])
        return {}
    finally:
        cursor.close()


def get_recent_ingestion_stats(
    conn: snowflake.connector.SnowflakeConnection,
    minutes: int = 10
) -> dict:
    """Get recent ingestion statistics."""
    cursor = conn.cursor()
    try:
        query = f"""
        SELECT
            COUNT(*) as file_count,
            SUM(ROW_COUNT) as total_rows,
            SUM(CASE WHEN STATUS = 'Loaded' THEN 1 ELSE 0 END) as loaded_count,
            SUM(CASE WHEN STATUS != 'Loaded' THEN 1 ELSE 0 END) as error_count,
            MAX(LAST_LOAD_TIME) as last_load_time
        FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
            TABLE_NAME => 'RAW_TRADES',
            START_TIME => DATEADD(MINUTE, -{minutes}, CURRENT_TIMESTAMP())
        ))
        """
        cursor.execute(query)
        row = cursor.fetchone()
        if row:
            return {
                'file_count': row[0] or 0,
                'total_rows': row[1] or 0,
                'loaded_count': row[2] or 0,
                'error_count': row[3] or 0,
                'last_load_time': row[4]
            }
        return {'file_count': 0, 'total_rows': 0, 'loaded_count': 0, 'error_count': 0, 'last_load_time': None}
    finally:
        cursor.close()


def check_data_freshness(
    conn: snowflake.connector.SnowflakeConnection,
    max_staleness_minutes: int = 15
) -> dict:
    """Check if data is fresh enough to proceed."""
    cursor = conn.cursor()
    try:
        query = """
        SELECT
            MAX(ingestion_timestamp) as latest_ingestion,
            DATEDIFF('minute', TO_TIMESTAMP_NTZ(MAX(ingestion_timestamp) / 1000), CURRENT_TIMESTAMP()) as staleness_minutes,
            COUNT(*) as total_records
        FROM RAW_TRADES
        """
        cursor.execute(query)
        row = cursor.fetchone()
        if row:
            return {
                'latest_ingestion': row[0],
                'staleness_minutes': row[1] or 999,
                'total_records': row[2] or 0,
                'is_fresh': (row[1] or 999) <= max_staleness_minutes
            }
        return {'latest_ingestion': None, 'staleness_minutes': 999, 'total_records': 0, 'is_fresh': False}
    finally:
        cursor.close()


def check_snowpipe_health(
    fail_on_error: bool = True,
    skip_on_no_data: bool = True,
    max_staleness_minutes: int = 15,
    min_recent_files: int = 0,
    **context
) -> dict:
    """
    Comprehensive Snowpipe health check for Airflow tasks.
    
    Args:
        fail_on_error: Raise AirflowFailException if pipe has errors
        skip_on_no_data: Raise AirflowSkipException if no recent data
        max_staleness_minutes: Max allowed data staleness
        min_recent_files: Minimum files expected in last 10 minutes
        context: Airflow context (automatically passed by PythonOperator)
    
    Returns:
        dict with health status information
    
    Raises:
        AirflowFailException: If pipe has critical errors
        AirflowSkipException: If no new data to process
    """
    conn = get_snowflake_connection()
    try:
        # Check pipe status
        pipe_status = get_pipe_status(conn)
        execution_state = pipe_status.get('executionState', 'UNKNOWN')
        pending_file_count = pipe_status.get('pendingFileCount', 0)
        
        logger.info(f"Snowpipe status: {execution_state}, pending files: {pending_file_count}")
        
        if execution_state == 'PAUSED':
            msg = "Snowpipe is PAUSED - data ingestion is stopped"
            logger.error(msg)
            if fail_on_error:
                raise AirflowFailException(msg)
        
        # Check recent ingestion stats
        ingestion_stats = get_recent_ingestion_stats(conn, minutes=10)
        logger.info(f"Recent ingestion: {ingestion_stats['file_count']} files, {ingestion_stats['total_rows']} rows")
        
        if ingestion_stats['error_count'] > 0:
            msg = f"Snowpipe has {ingestion_stats['error_count']} ingestion errors in last 10 minutes"
            logger.warning(msg)
            if fail_on_error and ingestion_stats['error_count'] > ingestion_stats['loaded_count']:
                raise AirflowFailException(msg)
        
        # Check data freshness
        freshness = check_data_freshness(conn, max_staleness_minutes)
        logger.info(f"Data staleness: {freshness['staleness_minutes']} minutes")
        
        # Skip if no new data and configured to skip
        if skip_on_no_data and ingestion_stats['file_count'] < min_recent_files:
            if freshness['staleness_minutes'] > max_staleness_minutes:
                msg = f"No recent data (staleness: {freshness['staleness_minutes']} min) - skipping run"
                logger.info(msg)
                raise AirflowSkipException(msg)
        
        health_result = {
            'status': 'healthy',
            'pipe_state': execution_state,
            'pending_files': pending_file_count,
            'recent_files': ingestion_stats['file_count'],
            'recent_rows': ingestion_stats['total_rows'],
            'error_count': ingestion_stats['error_count'],
            'staleness_minutes': freshness['staleness_minutes'],
            'is_fresh': freshness['is_fresh'],
            'checked_at': datetime.utcnow().isoformat()
        }
        
        # Push to XCom for downstream tasks
        if context.get('ti'):
            context['ti'].xcom_push(key='snowpipe_health', value=health_result)
        
        return health_result
        
    finally:
        conn.close()


def check_snowpipe_for_backfill(
    expected_hours: int = 24,
    min_expected_records: int = 1000,
    **context
) -> dict:
    """
    Enhanced health check for backfill operations.
    Verifies sufficient data exists for the backfill window.
    
    Args:
        expected_hours: Hours of data to verify
        min_expected_records: Minimum expected records in window
        context: Airflow context
    
    Returns:
        dict with backfill readiness information
    """
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        
        # Check data availability for backfill window
        query = f"""
        SELECT
            COUNT(*) as record_count,
            COUNT(DISTINCT trade_id) as unique_trades,
            COUNT(DISTINCT symbol) as unique_symbols,
            MIN(event_timestamp) as min_event_ts,
            MAX(event_timestamp) as max_event_ts,
            COUNT(DISTINCT DATE(TO_TIMESTAMP_NTZ(event_timestamp / 1000))) as distinct_dates
        FROM RAW_TRADES
        WHERE event_timestamp >= DATEDIFF('millisecond', '1970-01-01'::timestamp_ntz, 
                                          DATEADD('hour', -{expected_hours}, CURRENT_TIMESTAMP()))
        """
        cursor.execute(query)
        row = cursor.fetchone()
        
        result = {
            'record_count': row[0] or 0,
            'unique_trades': row[1] or 0,
            'unique_symbols': row[2] or 0,
            'min_event_ts': row[3],
            'max_event_ts': row[4],
            'distinct_dates': row[5] or 0,
            'expected_hours': expected_hours,
            'is_ready': (row[0] or 0) >= min_expected_records
        }
        
        logger.info(f"Backfill data check: {result['record_count']} records, "
                   f"{result['unique_trades']} unique trades across {result['distinct_dates']} dates")
        
        if not result['is_ready']:
            raise AirflowFailException(
                f"Insufficient data for backfill: {result['record_count']} records "
                f"(expected >= {min_expected_records})"
            )
        
        if context.get('ti'):
            context['ti'].xcom_push(key='backfill_data_check', value=result)
        
        cursor.close()
        return result
        
    finally:
        conn.close()
