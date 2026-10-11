#!/usr/bin/env python3
"""
Landing to Bronze Spark Job
============================

Production Spark job that reads Parquet from S3 landing bucket
and writes to Iceberg Bronze layer.

USAGE:
------
Local development:
    python landing_to_bronze.py --env local
    
With floci (AWS emulator):
    python landing_to_bronze.py --env floci

With specific layer:
    python landing_to_bronze.py --env floci --layer bronze
    python landing_to_bronze.py --env floci --layer silver
    python landing_to_bronze.py --env floci --layer gold

Full refresh (overwrite):
    python landing_to_bronze.py --env local --mode overwrite
    
Incremental (append):
    python landing_to_bronze.py --env local --mode append

Dry run (validate only):
    python landing_to_bronze.py --env local --dry-run
"""

import argparse
import sys
from datetime import datetime, timezone

from config import get_config, PipelineConfig
from utils import (
    create_spark_session,
    setup_logging,
    read_landing_data,
    validate_dataframe,
    write_to_iceberg,
    verify_write,
)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Landing to Bronze Spark ETL Job",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Local development (full refresh)
    python landing_to_bronze.py --env local --mode overwrite
    
    # Incremental load
    python landing_to_bronze.py --env local --mode append
    
    # Test with floci
    python landing_to_bronze.py --env floci
    
    # Dry run (validate only, no write)
    python landing_to_bronze.py --env local --dry-run
        """
    )
    
    parser.add_argument(
        "--env",
        choices=["local", "floci"],
        default="local",
        help="Environment to run in (default: local)"
    )
    
    parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "gold"],
        default="bronze",
        help="Medallion layer to write to (default: bronze)"
    )
    
    parser.add_argument(
        "--mode",
        choices=["overwrite", "append"],
        default="overwrite",
        help="Write mode (default: overwrite)"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate data but don't write to Iceberg"
    )
    
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)"
    )
    
    return parser.parse_args()


def main() -> int:
    """
    Main entry point for the landing-to-bronze job.
    
    Returns:
        Exit code: 0 for success, 1 for failure
    """
    # -------------------------------------------------------------------------
    # Step 1: Parse arguments and setup
    # -------------------------------------------------------------------------
    args = parse_args()
    
    logger = setup_logging("landing_to_bronze", args.log_level)
    logger.info("=" * 60)
    logger.info("LANDING TO BRONZE SPARK JOB")
    logger.info("=" * 60)
    logger.info(f"Environment: {args.env}")
    logger.info(f"Layer: {args.layer}")
    logger.info(f"Write mode: {args.mode}")
    logger.info(f"Dry run: {args.dry_run}")
    logger.info(f"Start time: {datetime.now(timezone.utc).isoformat()}")
    
    # -------------------------------------------------------------------------
    # Step 2: Load configuration
    # -------------------------------------------------------------------------
    try:
        config = get_config(args.env, args.layer)
        logger.info(f"Configuration loaded for: {args.env}/{args.layer}")
        logger.info(f"  S3 Bucket: {config.s3.bucket}")
        logger.info(f"  Landing Path: {config.landing_path}")
        logger.info(f"  Catalog: {config.iceberg.catalog}")
        logger.info(f"  Target Table: {config.iceberg.full_table_name}")
        logger.info(f"  Warehouse: {config.iceberg.warehouse_path}")
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        return 1
    
    # -------------------------------------------------------------------------
    # Step 3: Create Spark session
    # -------------------------------------------------------------------------
    try:
        spark = create_spark_session(config)
        logger.info(f"Spark session created: {spark.sparkContext.appName}")
        logger.info(f"Spark version: {spark.version}")
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        return 1
    
    # -------------------------------------------------------------------------
    # Step 4: Read landing data
    # -------------------------------------------------------------------------
    try:
        df_landing = read_landing_data(spark, config.landing_path, logger)
        if df_landing is None:
            logger.error("No data returned from landing bucket")
            spark.stop()
            return 1
        
        # Cache for reuse (validation + write)
        df_landing.cache()
        record_count = df_landing.count()
        logger.info(f"Read {record_count:,} records from landing")
        
    except Exception as e:
        logger.error(f"Failed to read landing data: {e}")
        spark.stop()
        return 1
    
    # -------------------------------------------------------------------------
    # Step 5: Validate data quality
    # -------------------------------------------------------------------------
    is_valid, errors = validate_dataframe(df_landing, logger=logger)
    
    if not is_valid:
        logger.warning("Data validation found issues:")
        for err in errors:
            logger.warning(f"  - {err}")
        # Bronze layer: continue anyway, handle issues in Silver
        logger.info("Continuing with write (Bronze layer accepts raw data)")
    
    # -------------------------------------------------------------------------
    # Step 6: Write to Iceberg (unless dry-run)
    # -------------------------------------------------------------------------
    if args.dry_run:
        logger.info("DRY RUN: Skipping write to Iceberg")
        logger.info(f"Would write {record_count:,} records to {config.iceberg.full_table_name}")
    else:
        success = write_to_iceberg(
            df_landing,
            config.iceberg.full_table_name,
            mode=args.mode,
            logger=logger
        )
        
        if not success:
            logger.error("Failed to write to Iceberg")
            df_landing.unpersist()
            spark.stop()
            return 1
    
    # -------------------------------------------------------------------------
    # Step 7: Verify write
    # -------------------------------------------------------------------------
    if not args.dry_run:
        verified = verify_write(
            spark,
            df_landing,
            config.iceberg.full_table_name,
            logger=logger
        )
        
        if not verified:
            logger.warning("Write verification failed - counts don't match")
    
    # -------------------------------------------------------------------------
    # Step 8: Cleanup and finish
    # -------------------------------------------------------------------------
    df_landing.unpersist()
    spark.stop()
    
    logger.info("=" * 60)
    logger.info("JOB COMPLETED SUCCESSFULLY")
    logger.info(f"End time: {datetime.now(timezone.utc).isoformat()}")
    logger.info("=" * 60)
    
    return 0


# =============================================================================
# Entry Point
# =============================================================================

if __name__ == "__main__":
    sys.exit(main())
