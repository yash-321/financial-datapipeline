"""Trades consumer - consumes trade events from Kafka and writes to S3.

This consumer:
1. Consumes Avro-encoded trade events from Kafka
2. Buffers and batches records by date/symbol partitions
3. Writes Parquet files with Hive-style partitioning
4. Optionally uploads to S3 with async workers
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
import os
import time
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

from .base import BaseConsumer, ConsumerConfig, setup_signal_handlers
from .uploaders.s3 import S3Uploader


class TradesConsumer(BaseConsumer):
    """Consumer for trade events with date/symbol partitioning."""

    # PyArrow schema for parquet output
    ARROW_SCHEMA = pa.schema([
        ('symbol', pa.string()),
        ('trade_id', pa.string()),
        ('price', pa.float64()),
        ('quantity', pa.float64()),
        ('event_timestamp', pa.int64()),
        ('ingestion_timestamp', pa.int64()),
    ])

    def __init__(
        self,
        config: ConsumerConfig,
        s3_bucket: Optional[str] = None,
        s3_prefix: str = 'trades',
        s3_region: Optional[str] = None,
        s3_endpoint_url: Optional[str] = None,
        delete_local_after_upload: bool = False,
        upload_workers: int = 4,
    ):
        """Initialize trades consumer with optional S3 upload.
        
        Args:
            config: Base consumer configuration
            s3_bucket: S3 bucket for uploads (None disables S3)
            s3_prefix: S3 key prefix
            s3_region: AWS region
            s3_endpoint_url: Custom S3 endpoint URL (for Floci/LocalStack emulators)
            delete_local_after_upload: Delete local files after S3 upload
            upload_workers: Number of async upload workers
        """
        super().__init__(config)
        
        # S3 configuration
        self.s3_enabled = s3_bucket is not None
        self.delete_local_after_upload = delete_local_after_upload
        self._s3_uploader: Optional[S3Uploader] = None

        if s3_bucket:
            self._s3_uploader = S3Uploader(
                bucket=s3_bucket,
                prefix=s3_prefix,
                region=s3_region,
                endpoint_url=s3_endpoint_url,
            )
            if self._s3_uploader.check_connection():
                self.logger.info(f"S3 upload enabled: s3://{s3_bucket}/{s3_prefix}/")
                self._s3_uploader.start_async_workers(num_workers=upload_workers)
            else:
                self.logger.error("S3 connection check failed. Uploads disabled.")
                self.s3_enabled = False

    def get_partition_keys(self, record: dict) -> tuple[str, str]:
        """Extract date and symbol from trade record for partitioning."""
        timestamp_ms = record['event_timestamp']
        date_str = datetime.fromtimestamp(
            timestamp_ms / 1000, tz=timezone.utc
        ).strftime('%Y-%m-%d')
        return (f"date={date_str}", f"symbol={record['symbol']}")

    def process_batch(self, partition_key: str, records: list[dict]) -> Path:
        """Write a batch of trade records to a parquet file.
        
        Args:
            partition_key: e.g., "date=2026-05-01/symbol=AAPL"
            records: List of trade records
            
        Returns:
            Path to the written parquet file
        """
        # Build partition directory
        partition_dir = self.output_dir / partition_key
        partition_dir.mkdir(parents=True, exist_ok=True)

        # Generate filename with timestamp and UUID
        timestamp = int(time.time() * 1000)
        unique_id = str(uuid.uuid4())[:8]
        filename = f"part-{timestamp}-{unique_id}.parquet"
        filepath = partition_dir / filename
        temp_filepath = partition_dir / f".{filename}.tmp"

        # Convert to PyArrow table and write
        table = pa.Table.from_pylist(records, schema=self.ARROW_SCHEMA)
        
        # Write to temp file first (atomic write pattern)
        pq.write_table(
            table,
            temp_filepath,
            compression='snappy',
            row_group_size=10_000,
            use_dictionary=False,
        )
        
        # Atomic rename on success
        temp_filepath.rename(filepath)
        self.logger.info(f"Written {len(records)} records to {filepath}")

        # Queue S3 upload if enabled
        if self.s3_enabled and self._s3_uploader:
            queued = self._s3_uploader.queue_upload(
                local_path=filepath,
                base_dir=self.output_dir,
                delete_after_upload=self.delete_local_after_upload,
            )
            if not queued:
                # Queue full - fallback to sync upload
                self.logger.warning(f"Queue full, sync uploading {filepath}")
                self._s3_uploader.upload_file(
                    local_path=filepath,
                    base_dir=self.output_dir,
                    delete_after_upload=self.delete_local_after_upload,
                )

        return filepath

    def stop(self) -> None:
        """Stop consumer, flush data, and wait for S3 uploads."""
        super().stop()
        
        # Wait for pending S3 uploads
        if self.s3_enabled and self._s3_uploader:
            self._s3_uploader.wait_for_uploads()


def main():
    """Main entry point for trades consumer."""
    load_dotenv()

    # Build configuration from environment
    config = ConsumerConfig(
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'),
        topic=os.getenv('TRADES_TOPIC', 'trades_topic'),
        group_id=os.getenv('TRADES_GROUP_ID', 'trade_consumer_group'),
        schema_path=os.getenv('TRADES_SCHEMA_PATH', '/app/configs/schemas/trade_schema.avsc'),
        output_dir=os.getenv('TRADES_OUTPUT_DIR', '/app/data/trades'),
        flush_interval_seconds=int(os.getenv('FLUSH_INTERVAL_SECONDS', '30')),
        max_buffer_size=int(os.getenv('MAX_BUFFER_SIZE', '100000')),
    )

    # S3 configuration
    s3_bucket = os.getenv('S3_BUCKET')
    s3_prefix = os.getenv('S3_PREFIX', 'trades')
    s3_region = os.getenv('AWS_REGION')
    s3_endpoint_url = os.getenv('AWS_ENDPOINT_URL')
    delete_local = os.getenv('DELETE_LOCAL_AFTER_UPLOAD', 'false').lower() == 'true'

    consumer = TradesConsumer(
        config=config,
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        s3_region=s3_region,
        s3_endpoint_url=s3_endpoint_url,
        delete_local_after_upload=delete_local,
    )

    if s3_bucket:
        endpoint_info = f" (endpoint: {s3_endpoint_url})" if s3_endpoint_url else ""
        consumer.logger.info(f"S3 upload configured: s3://{s3_bucket}/{s3_prefix}/{endpoint_info}")
    else:
        consumer.logger.info("S3 upload disabled (S3_BUCKET not set)")

    # Setup graceful shutdown
    setup_signal_handlers(consumer)

    # Start consuming
    consumer.start()


if __name__ == "__main__":
    main()
