"""
Utilities Module for Spark Jobs
================================

This module provides reusable utilities for Spark jobs:
- Spark session creation with Iceberg support
- Data validation functions
- Logging setup
- Error handling helpers
"""

import logging
import sys
from typing import Optional, List, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when, isnan, isnull

from config import PipelineConfig


def create_spark_session(config: PipelineConfig) -> SparkSession:
    """
    Create a SparkSession configured for Iceberg and S3 access.
    
    Args:
        config: PipelineConfig object with all settings
        
    Returns:
        Configured SparkSession
    """
    # Start building the SparkSession
    builder = SparkSession.builder.appName(config.spark.app_name)
    
    # Add JAR packages for Iceberg + S3
    packages = ",".join(config.spark.packages)
    builder = builder.config("spark.jars.packages", packages)
    
    # Configure Iceberg extensions
    builder = builder.config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    
    # Configure Iceberg catalog
    catalog_name = config.iceberg.catalog
    builder = builder.config(
        f"spark.sql.catalog.{catalog_name}",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    
    # Configure catalog implementation based on type
    if config.iceberg.catalog_type == "glue":
        # AWS Glue Catalog configuration
        builder = builder.config(
            f"spark.sql.catalog.{catalog_name}.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog"
        )
        builder = builder.config(
            f"spark.sql.catalog.{catalog_name}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO"
        )
    else:
        # Hadoop catalog configuration
        builder = builder.config(
            f"spark.sql.catalog.{catalog_name}.type",
            config.iceberg.catalog_type
        )
    
    builder = builder.config(
        f"spark.sql.catalog.{catalog_name}.warehouse",
        config.iceberg.warehouse_path
    )
    
    # Configure S3 access
    if config.s3.access_key:
        builder = builder.config("spark.hadoop.fs.s3a.access.key", config.s3.access_key)
    if config.s3.secret_key:
        builder = builder.config("spark.hadoop.fs.s3a.secret.key", config.s3.secret_key)
    
    builder = builder.config(
        "spark.hadoop.fs.s3a.endpoint",
        config.s3.endpoint
    )
    builder = builder.config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    
    # For floci (LocalStack), enable path-style access and disable SSL
    if config.env == "floci":
        builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
        builder = builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Configure Glue endpoint for floci
        if config.iceberg.catalog_type == "glue":
            builder = builder.config(
                f"spark.sql.catalog.{config.iceberg.catalog}.glue.endpoint",
                config.s3.endpoint
            )
            # Configure S3FileIO for Iceberg writes (uses AWS SDK v2, not Hadoop S3A)
            builder = builder.config(
                f"spark.sql.catalog.{config.iceberg.catalog}.s3.endpoint",
                config.s3.endpoint
            )
            builder = builder.config(
                f"spark.sql.catalog.{config.iceberg.catalog}.s3.path-style-access",
                "true"
            )
            builder = builder.config(
                f"spark.sql.catalog.{config.iceberg.catalog}.s3.access-key-id",
                config.s3.access_key
            )
            builder = builder.config(
                f"spark.sql.catalog.{config.iceberg.catalog}.s3.secret-access-key",
                config.s3.secret_key
            )
    
    # Configure memory and parallelism
    builder = builder.config("spark.driver.memory", config.spark.driver_memory)
    builder = builder.config("spark.sql.shuffle.partitions", str(config.spark.shuffle_partitions))
    
    # Create and return the session
    spark = builder.getOrCreate()
    return spark


def setup_logging(name: str = "landing_to_bronze", level: str = "INFO") -> logging.Logger:
    """
    Setup structured logging for the job.
    
    Args:
        name: Logger name
        level: Log level (DEBUG, INFO, WARNING, ERROR)
        
    Returns:
        Configured logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    
    # Create console handler with formatter
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, level.upper()))
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    
    # Add handler to logger (avoid duplicates)
    if not logger.handlers:
        logger.addHandler(handler)
    
    return logger


def read_landing_data(
    spark: SparkSession,
    landing_path: str,
    logger: Optional[logging.Logger] = None
) -> Optional[DataFrame]:
    """
    Read Parquet data from S3 landing bucket.
    
    Args:
        spark: SparkSession
        landing_path: S3 path to landing data (e.g., s3a://bucket/trades/)
        logger: Optional logger for progress messages
        
    Returns:
        DataFrame with landing data, or None if error
    """
    if logger:
        logger.info(f"Reading landing data from: {landing_path}")
    
    try:
        df = spark.read \
            .option("inferSchema", "true") \
            .option("basePath", landing_path) \
            .parquet(landing_path)
        
        if logger:
            count = df.count()
            logger.info(f"Successfully read {count:,} records")
        
        return df
        
    except Exception as e:
        if logger:
            logger.error(f"Failed to read landing data: {e}")
        raise


def validate_dataframe(
    df: DataFrame,
    required_columns: Optional[List[str]] = None,
    logger: Optional[logging.Logger] = None
) -> Tuple[bool, List[str]]:
    """
    Validate DataFrame before writing.
    
    Args:
        df: DataFrame to validate
        required_columns: List of columns that must not be null
        logger: Optional logger
        
    Returns:
        Tuple of (is_valid: bool, errors: List[str])
    """
    if required_columns is None:
        required_columns = ["symbol", "trade_id", "price", "quantity"]
    
    errors = []
    
    # Check 1 - DataFrame not empty
    record_count = df.count()
    if record_count == 0:
        errors.append("DataFrame is empty")
        return False, errors
    
    # Check 2 - No nulls in required columns
    for col_name in required_columns:
        if col_name in df.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            if null_count > 0:
                errors.append(f"Found {null_count} NULL values in {col_name}")
    
    # Check 3 - Positive prices
    if "price" in df.columns:
        negative_prices = df.filter(col("price") <= 0).count()
        if negative_prices > 0:
            errors.append(f"Found {negative_prices} non-positive prices")
    
    # Check 4 - Positive quantities
    if "quantity" in df.columns:
        negative_qty = df.filter(col("quantity") <= 0).count()
        if negative_qty > 0:
            errors.append(f"Found {negative_qty} non-positive quantities")
    
    # Log results
    if logger:
        if errors:
            for err in errors:
                logger.warning(f"Validation issue: {err}")
        else:
            logger.info(f"Validation passed for {record_count:,} records")
    
    is_valid = len(errors) == 0
    return is_valid, errors


def write_to_iceberg(
    df: DataFrame,
    table_name: str,
    mode: str = "overwrite",
    logger: Optional[logging.Logger] = None
) -> bool:
    """
    Write DataFrame to Iceberg table.
    
    Args:
        df: DataFrame to write
        table_name: Fully qualified table name (catalog.database.table)
        mode: Write mode - "overwrite" or "append"
        logger: Optional logger
        
    Returns:
        True if successful, False otherwise
    """
    if logger:
        logger.info(f"Writing to Iceberg table: {table_name} (mode={mode})")
    
    # Select columns in correct order (match table schema)
    columns = [
        "symbol",
        "trade_id",
        "price",
        "quantity",
        "event_timestamp",
        "ingestion_timestamp",
        "date"
    ]
    df_write = df.select(columns)
    
    try:
        df_write.write \
            .format("iceberg") \
            .mode(mode) \
            .option("write.format.default", "parquet") \
            .option("write.parquet.compression-codec", "snappy") \
            .saveAsTable(table_name)
        
        if logger:
            logger.info(f"Successfully wrote to {table_name}")
        return True
        
    except Exception as e:
        if logger:
            logger.error(f"Failed to write to Iceberg: {e}")
        return False


def verify_write(
    spark: SparkSession,
    source_df: DataFrame,
    table_name: str,
    logger: Optional[logging.Logger] = None
) -> bool:
    """
    Verify write by comparing source and target counts.
    
    Args:
        spark: SparkSession
        source_df: Original DataFrame that was written
        table_name: Target table name
        logger: Optional logger
        
    Returns:
        True if counts match, False otherwise
    """
    # Get source count
    source_count = source_df.count()
    
    # Get target count
    target_count = spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
    
    # Compare and log
    match = source_count == target_count
    if logger:
        logger.info(f"Source count: {source_count:,}")
        logger.info(f"Target count: {target_count:,}")
        if match:
            logger.info("✓ Counts match - write verified")
        else:
            logger.warning("✗ Count mismatch - verification failed")
    
    return match


if __name__ == "__main__":
    print("Utils module loaded successfully!")
    print("Available functions:")
    print("  - create_spark_session(config)")
    print("  - setup_logging(name, level)")
    print("  - read_landing_data(spark, path, logger)")
    print("  - validate_dataframe(df, columns, logger)")
    print("  - write_to_iceberg(df, table, mode, logger)")
    print("  - verify_write(spark, df, table, logger)")
