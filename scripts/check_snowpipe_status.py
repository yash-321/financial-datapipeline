#!/usr/bin/env python3
"""
Snowpipe status monitoring script.

Checks the status of the trades_pipe Snowpipe and displays:
- Current pipe status (running/paused)
- Recent file ingestion history
- Any ingestion errors
- Data freshness metrics
"""

import os
import sys
import json
from datetime import datetime, timedelta
from typing import Optional

import snowflake.connector
from tabulate import tabulate

from dotenv import load_dotenv
load_dotenv()



def get_connection() -> snowflake.connector.SnowflakeConnection:
    """Create Snowflake connection from environment variables."""
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "DATAPIPELINE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "DATAPIPELINE"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "TRADES"),
        role=os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
    )


def check_pipe_status(conn: snowflake.connector.SnowflakeConnection) -> dict:
    """Get current pipe status."""
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT SYSTEM$PIPE_STATUS('trades_pipe')")
        result = cursor.fetchone()
        if result:
            return json.loads(result[0])
        return {}
    finally:
        cursor.close()


def get_copy_history(
    conn: snowflake.connector.SnowflakeConnection,
    hours: int = 24,
    limit: int = 20,
) -> list[dict]:
    """Get recent copy history for the trades pipe."""
    cursor = conn.cursor()
    try:
        query = f"""
        SELECT
            FILE_NAME,
            STATUS,
            ROW_COUNT,
            ROW_PARSED,
            FIRST_ERROR_MESSAGE,
            FIRST_ERROR_LINE_NUMBER,
            LAST_LOAD_TIME
        FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
            TABLE_NAME => 'RAW_TRADES',
            START_TIME => DATEADD(HOUR, -{hours}, CURRENT_TIMESTAMP())
        ))
        ORDER BY LAST_LOAD_TIME DESC
        LIMIT {limit}
        """
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        cursor.close()


def get_ingestion_errors(
    conn: snowflake.connector.SnowflakeConnection,
    hours: int = 24,
) -> list[dict]:
    """Get recent ingestion errors."""
    cursor = conn.cursor()
    try:
        query = f"""
        SELECT
            FILE_NAME,
            STATUS,
            FIRST_ERROR_MESSAGE,
            FIRST_ERROR_LINE_NUMBER,
            ERROR_COUNT,
            LAST_LOAD_TIME
        FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
            TABLE_NAME => 'RAW_TRADES',
            START_TIME => DATEADD(HOUR, -{hours}, CURRENT_TIMESTAMP())
        ))
        WHERE STATUS != 'Loaded'
        ORDER BY LAST_LOAD_TIME DESC
        """
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        cursor.close()


def get_data_freshness(conn: snowflake.connector.SnowflakeConnection) -> dict:
    """Get data freshness metrics."""
    cursor = conn.cursor()
    try:
        query = """
        SELECT
            COUNT(*) as total_rows,
            MIN(_loaded_at) as first_load,
            MAX(_loaded_at) as last_load,
            COUNT(DISTINCT trade_date) as distinct_dates,
            COUNT(DISTINCT symbol) as distinct_symbols,
            MAX(event_timestamp) as latest_event_ts
        FROM RAW_TRADES
        """
        cursor.execute(query)
        row = cursor.fetchone()
        if row:
            columns = [desc[0] for desc in cursor.description]
            return dict(zip(columns, row))
        return {}
    finally:
        cursor.close()


def get_recent_stats(
    conn: snowflake.connector.SnowflakeConnection,
    hours: int = 1,
) -> list[dict]:
    """Get ingestion stats for recent hours."""
    cursor = conn.cursor()
    try:
        query = f"""
        SELECT
            symbol,
            COUNT(*) as trade_count,
            MIN(event_timestamp) as min_event_ts,
            MAX(event_timestamp) as max_event_ts
        FROM RAW_TRADES
        WHERE _loaded_at >= DATEADD(HOUR, -{hours}, CURRENT_TIMESTAMP())
        GROUP BY symbol
        ORDER BY trade_count DESC
        """
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        cursor.close()


def format_timestamp(ts) -> str:
    """Format timestamp for display."""
    if ts is None:
        return "N/A"
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M:%S")
    return str(ts)


def main():
    """Main entry point."""
    print("=" * 60)
    print("Snowpipe Status Monitor - trades_pipe")
    print("=" * 60)
    print()

    try:
        conn = get_connection()
    except KeyError as e:
        print(f"Error: Missing environment variable: {e}")
        print("\nRequired environment variables:")
        print("  SNOWFLAKE_ACCOUNT")
        print("  SNOWFLAKE_USER")
        print("  SNOWFLAKE_PASSWORD")
        print("\nOptional (with defaults):")
        print("  SNOWFLAKE_WAREHOUSE (default: COMPUTE_WH)")
        print("  SNOWFLAKE_DATABASE (default: DATAPIPELINE)")
        print("  SNOWFLAKE_SCHEMA (default: TRADES)")
        print("  SNOWFLAKE_ROLE (default: SYSADMIN)")
        sys.exit(1)
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        sys.exit(1)

    try:
        # Pipe Status
        print("📊 Pipe Status")
        print("-" * 40)
        status = check_pipe_status(conn)
        if status:
            print(f"  Execution State: {status.get('executionState', 'Unknown')}")
            print(f"  Pending Files: {status.get('pendingFileCount', 0)}")
            print(f"  Last Ingested: {status.get('lastIngestedTimestamp', 'N/A')}")
        else:
            print("  Unable to retrieve pipe status")
        print()

        # Data Freshness
        print("📈 Data Freshness")
        print("-" * 40)
        freshness = get_data_freshness(conn)
        if freshness and freshness.get("TOTAL_ROWS", 0) > 0:
            print(f"  Total Rows: {freshness.get('TOTAL_ROWS', 0):,}")
            print(f"  First Load: {freshness.get('FIRST_LOAD', 'N/A')}")
            print(f"  Last Load: {freshness.get('LAST_LOAD', 'N/A')}")
            print(f"  Distinct Dates: {freshness.get('DISTINCT_DATES', 0)}")
            print(f"  Distinct Symbols: {freshness.get('DISTINCT_SYMBOLS', 0)}")
            latest_ts = freshness.get("LATEST_EVENT_TS")
            if latest_ts:
                print(f"  Latest Event: {format_timestamp(latest_ts)}")
        else:
            print("  No data loaded yet")
        print()

        # Recent Copy History
        print("📁 Recent Copy History (last 24 hours)")
        print("-" * 40)
        history = get_copy_history(conn, hours=24, limit=10)
        if history:
            table_data = [
                [
                    h["FILE_NAME"].split("/")[-1][:40],
                    h["STATUS"],
                    h["ROW_COUNT"] or 0,
                    h["LAST_LOAD_TIME"],
                ]
                for h in history
            ]
            print(tabulate(
                table_data,
                headers=["File", "Status", "Rows", "Load Time"],
                tablefmt="simple",
            ))
        else:
            print("  No files loaded in the last 24 hours")
        print()

        # Errors
        print("⚠️  Ingestion Errors (last 24 hours)")
        print("-" * 40)
        errors = get_ingestion_errors(conn, hours=24)
        if errors:
            for err in errors[:5]:
                print(f"  File: {err['FILE_NAME'].split('/')[-1]}")
                print(f"  Status: {err['STATUS']}")
                print(f"  Error: {err['FIRST_ERROR_MESSAGE']}")
                print()
        else:
            print("  No errors in the last 24 hours ✓")
        print()

        # Recent Stats by Symbol
        print("📊 Recent Activity (last 1 hour)")
        print("-" * 40)
        stats = get_recent_stats(conn, hours=1)
        if stats:
            table_data = [
                [s["SYMBOL"], f"{s['TRADE_COUNT']:,}"]
                for s in stats
            ]
            print(tabulate(
                table_data,
                headers=["Symbol", "Trades"],
                tablefmt="simple",
            ))
        else:
            print("  No trades loaded in the last hour")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
