"""
Configuration Module for Spark Jobs
====================================

This module centralizes all configuration for the landing-to-bronze pipeline.
It supports multiple environments: local development and floci (AWS emulator).
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, List
from dotenv import load_dotenv


@dataclass
class S3Config:
    """S3 configuration for landing bucket access."""
    
    bucket: str = ""
    region: str = "eu-west-1"
    endpoint: Optional[str] = None
    access_key: Optional[str] = None
    secret_key: Optional[str] = None


@dataclass
class IcebergConfig:
    """Iceberg catalog and table configuration."""
    
    catalog: str = "local"
    database: str = "default"
    table: str = "raw_trades"
    warehouse_path: str = ""
    catalog_type: str = "hadoop"  # "hadoop" or "glue"
    layer: str = "bronze"  # "bronze", "silver", or "gold"
    
    @property
    def full_table_name(self) -> str:
        """Returns fully qualified table name: catalog.database.table"""
        return f"{self.catalog}.{self.database}.{self.table}"


@dataclass
class SparkConfig:
    """Spark session configuration."""
    
    app_name: str = "landing-to-bronze"
    driver_memory: str = "4g"
    executor_memory: str = "4g"
    shuffle_partitions: int = 8
    packages: List[str] = field(default_factory=lambda: [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.1",
        "org.apache.iceberg:iceberg-aws-bundle:1.4.1",  # AWS Glue catalog support
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ])


@dataclass
class PipelineConfig:
    """Complete pipeline configuration."""
    
    env: str = "local"
    s3: S3Config = field(default_factory=S3Config)
    iceberg: IcebergConfig = field(default_factory=IcebergConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    landing_path: str = ""
    write_mode: str = "overwrite"


# Layer-specific table mappings
LAYER_CONFIG = {
    "bronze": {"database": "iceberg_bronze", "table": "raw_trades"},
    "silver": {"database": "iceberg_silver", "table": "trades"},
    "gold": {"database": "iceberg_gold", "table": "trade_summary"},
}


def get_config(env: str = "local", layer: str = "bronze") -> PipelineConfig:
    """
    Factory function to create configuration for the specified environment.
    
    Args:
        env: Environment name - "local" or "floci"
        layer: Medallion layer - "bronze", "silver", or "gold"
        
    Returns:
        PipelineConfig with all settings for the environment
    """
    if layer not in LAYER_CONFIG:
        raise ValueError(f"Unknown layer: {layer}. Use 'bronze', 'silver', or 'gold'")
    # Load environment variables from .env file
    env_path = Path(__file__).parent.parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    
    # Read credentials from environment
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_REGION", "eu-west-1")
    s3_bucket = os.getenv("S3_BUCKET", "financial-data-landing-587129419094-eu-west-1-an")
    
    # Warehouse path (relative to spark/ directory)
    warehouse_path = str(Path(__file__).parent.parent / "warehouse")
    
    if env == "local":
        # LOCAL configuration: real AWS S3, local Hadoop catalog
        s3_config = S3Config(
            bucket=s3_bucket,
            region=aws_region,
            endpoint=f"s3.{aws_region}.amazonaws.com",
            access_key=aws_access_key,
            secret_key=aws_secret_key,
        )
        
        iceberg_config = IcebergConfig(
            catalog="local",
            database="default",
            table="raw_trades",
            warehouse_path=warehouse_path,
            catalog_type="hadoop",
        )
        
        landing_path = f"s3a://{s3_bucket}/trades/"
        
        return PipelineConfig(
            env="local",
            s3=s3_config,
            iceberg=iceberg_config,
            spark=SparkConfig(),
            landing_path=landing_path,
            write_mode="overwrite",
        )
        
    elif env == "floci":
        # FLOCI configuration: LocalStack S3 emulator + Glue catalog
        floci_bucket = os.getenv("FLOCI_BUCKET", "landing-bucket")
        warehouse_bucket = os.getenv("FLOCI_WAREHOUSE_BUCKET", "iceberg-warehouse")
        
        # Get layer-specific database and table
        layer_db = LAYER_CONFIG[layer]["database"]
        layer_table = LAYER_CONFIG[layer]["table"]
        
        s3_config = S3Config(
            bucket=floci_bucket,
            region="us-east-1",
            endpoint="http://localhost:4566",
            access_key="test",
            secret_key="test",
        )
        
        # Use S3 warehouse with Glue catalog for floci
        s3_warehouse_path = f"s3a://{warehouse_bucket}/{layer}"
        
        iceberg_config = IcebergConfig(
            catalog="glue_catalog",
            database=layer_db,
            table=layer_table,
            warehouse_path=s3_warehouse_path,
            catalog_type="glue",
            layer=layer,
        )
        
        landing_path = f"s3a://{floci_bucket}/trades/"
        
        return PipelineConfig(
            env="floci",
            s3=s3_config,
            iceberg=iceberg_config,
            spark=SparkConfig(),
            landing_path=landing_path,
            write_mode="overwrite",
        )
        
    else:
        raise ValueError(f"Unknown environment: {env}. Use 'local' or 'floci'")


#
# =============================================================================
# Main block for testing the config
# =============================================================================

if __name__ == "__main__":
    print("Testing LOCAL configuration:")
    local_config = get_config("local", "bronze")
    print(f"  Environment: {local_config.env}")
    print(f"  S3 Bucket: {local_config.s3.bucket}")
    print(f"  S3 Region: {local_config.s3.region}")
    print(f"  Iceberg Table: {local_config.iceberg.full_table_name}")
    print(f"  Warehouse Path: {local_config.iceberg.warehouse_path}")
    print(f"  Catalog Type: {local_config.iceberg.catalog_type}")
    print(f"  Layer: {local_config.iceberg.layer}")
    print(f"  Landing Path: {local_config.landing_path}")
    print(f"  Credentials: {'Set' if local_config.s3.access_key else 'Not set'}")
    
    print("\nTesting FLOCI configuration (all layers):")
    for layer in ["bronze", "silver", "gold"]:
        floci_config = get_config("floci", layer)
        print(f"  [{layer.upper()}]")
        print(f"    Catalog: {floci_config.iceberg.catalog}")
        print(f"    Database: {floci_config.iceberg.database}")
        print(f"    Table: {floci_config.iceberg.table}")
        print(f"    Warehouse: {floci_config.iceberg.warehouse_path}")
    
    print("\n✅ Configuration module working correctly!")

