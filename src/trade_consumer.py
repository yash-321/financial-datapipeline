from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
import copy
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from queue import Queue, Empty
from typing import Callable, Optional
from dotenv import load_dotenv
import fastavro
import io
import json
import logging
import os
import pyarrow as pa
import pyarrow.parquet as pq
import signal
import threading
import time
import uuid

from s3_uploader import S3Uploader



logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class UploadTask:
    """Represents an S3 upload task."""
    local_path: Path
    base_dir: Path
    delete_after_upload: bool
    retry_count: int = 0


class TradeConsumer:
    """Kafka consumer for trade events with parquet output."""

    MAX_UPLOAD_RETRIES = 3
    UPLOAD_QUEUE_MAX_SIZE = 1000

    def __init__(
        self,
        bootstrap_servers: str = 'localhost:9092',
        topic: str = 'trades_topic',
        group_id: str = 'trade_consumer_group',
        schema_path: str = 'configs/trade_schema.avsc',
        output_dir: str = 'data/trades',
        flush_interval_seconds: int = 30,
        max_buffer_size: int = 100_000,  # Max records per partition before force flush
        # S3 upload configuration (optional)
        s3_bucket: Optional[str] = None,
        s3_prefix: str = 'trades',
        s3_region: Optional[str] = None,
        delete_local_after_upload: bool = False,
        upload_workers: int = 4,
    ):
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,  # Manual commit for failure safety
        }
        self.consumer = Consumer(self.config)
        self.topic = topic
        self.output_dir = Path(output_dir)
        self.flush_interval = flush_interval_seconds
        self.max_buffer_size = max_buffer_size

        # Partitioned buffer: date -> symbol -> list of records
        self.buffer: dict[str, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))
        self.buffer_lock = threading.Lock()
        self.buffer_count = 0

        # Load Avro schema
        self.schema = self._load_schema(schema_path)

        # PyArrow schema for parquet (optional, for reference)
        # Let Arrow infer schema from data to avoid crashes on missing fields
        self.arrow_schema = pa.schema([
            ('symbol', pa.string()),
            ('trade_id', pa.string()),
            ('price', pa.float64()),
            ('quantity', pa.float64()),
            ('event_timestamp', pa.int64()),
            ('ingestion_timestamp', pa.int64()),
        ])

        # Control flags
        self._running = False
        self._flush_thread: Optional[threading.Thread] = None
        self._last_flush_time = time.time()

        # Pending offsets to commit after successful flush
        # Track only latest offset per partition: (topic, partition) -> offset
        self._pending_offsets: dict[tuple[str, int], int] = {}

        # S3 upload configuration
        self.s3_enabled = s3_bucket is not None
        self.delete_local_after_upload = delete_local_after_upload
        self._s3_uploader: Optional['S3Uploader'] = None

        # Upload queue and worker threads
        self._upload_queue: Queue[Optional[UploadTask]] = Queue(maxsize=self.UPLOAD_QUEUE_MAX_SIZE)
        self._upload_workers: list[threading.Thread] = []
        self._uploads_in_flight: int = 0
        self._uploads_lock: threading.Lock = threading.Lock()
        self._num_upload_workers = upload_workers

        if s3_bucket:
            self._s3_uploader = S3Uploader(
                bucket=s3_bucket,
                prefix=s3_prefix,
                region=s3_region,
            )
            if self._s3_uploader.check_connection():
                logger.info(f"S3 upload enabled: s3://{s3_bucket}/{s3_prefix}/")
                # Start upload worker threads
                for i in range(self._num_upload_workers):
                    worker = threading.Thread(
                        target=self._upload_worker,
                        name=f"upload-worker-{i}",
                        daemon=True
                    )
                    worker.start()
                    self._upload_workers.append(worker)
                logger.info(f"Started {self._num_upload_workers} upload worker threads")
            else:
                logger.error("S3 connection check failed. Uploads may fail.")
                self.s3_enabled = False

    def _load_schema(self, schema_path: str) -> dict:
        """Load Avro schema from file."""
        with open(schema_path, 'r') as f:
            return json.load(f)

    def deserialize(self, data: bytes) -> dict:
        """Deserialize Avro-encoded trade data."""
        buffer = io.BytesIO(data)
        return fastavro.schemaless_reader(buffer, self.schema)

    def _get_date_key(self, timestamp_ms: int) -> str:
        """Convert millisecond timestamp to date string for partitioning."""
        return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).strftime('%Y-%m-%d')

    def _add_to_buffer(self, trade: dict) -> None:
        """Add a trade to the partitioned buffer."""
        date_key = self._get_date_key(trade['event_timestamp'])
        symbol = trade['symbol']

        with self.buffer_lock:
            self.buffer[date_key][symbol].append(trade)
            self.buffer_count += 1

    def _should_flush(self) -> bool:
        """Check if buffer should be flushed based on time or size."""
        time_exceeded = (time.time() - self._last_flush_time) >= self.flush_interval
        size_exceeded = self.buffer_count >= self.max_buffer_size
        return (time_exceeded or size_exceeded) and self.buffer_count > 0

    def _generate_filename(self) -> str:
        """Generate parquet filename with timestamp and UUID."""
        timestamp = int(time.time() * 1000)
        unique_id = str(uuid.uuid4())[:8]
        return f"part-{timestamp}-{unique_id}.parquet"

    def _write_partition(self, date_key: str, symbol: str, records: list[dict]) -> Path:
        """Write records to a parquet file for a specific partition.
        
        Uses temp file + rename pattern to avoid partial writes on failure.
        """
        # Create partition directory structure: output_dir/date=YYYY-MM-DD/symbol=XXX/
        partition_dir = self.output_dir / f"date={date_key}" / f"symbol={symbol}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        filename = self._generate_filename()
        filepath = partition_dir / filename
        temp_filepath = partition_dir / f".{filename}.tmp"

        # Convert to PyArrow Table using schema without partition columns
        table = pa.Table.from_pylist(records, schema=self.arrow_schema)

        # Write to temp file first
        pq.write_table(
            table,
            temp_filepath,
            compression='snappy',
            row_group_size=10_000,  # Control memory during write
            use_dictionary=False
        )

        # Atomic rename on success
        temp_filepath.rename(filepath)

        logger.info(f"Written {len(records)} records to {filepath}")

        # Queue upload to S3 if enabled
        if self.s3_enabled and self._s3_uploader:
            self._queue_upload(filepath)

        return filepath

    def _upload_worker(self) -> None:
        """Worker thread that processes uploads from the queue."""
        while True:
            try:
                task = self._upload_queue.get()

                # Shutdown sentinel
                if task is None:
                    self._upload_queue.task_done()
                    break

                with self._uploads_lock:
                    self._uploads_in_flight += 1

                try:
                    self._process_upload(task)
                finally:
                    with self._uploads_lock:
                        self._uploads_in_flight -= 1
                    self._upload_queue.task_done()

            except Exception as e:
                logger.error(f"Upload worker error: {e}")

    def _process_upload(self, task: UploadTask) -> None:
        """Process a single upload task."""
        try:
            success, s3_uri = self._s3_uploader.upload_file(
                local_path=task.local_path,
                base_dir=task.base_dir,
                delete_after_upload=task.delete_after_upload,
            )
            if success:
                logger.debug(f"Successfully uploaded {task.local_path} to {s3_uri}")
            else:
                self._handle_upload_failure(task, Exception("Upload returned failure"))
        except Exception as e:
            self._handle_upload_failure(task, e)

    def _handle_upload_failure(self, task: UploadTask, error: Exception) -> None:
        """Handle upload failure with exponential backoff retry."""
        if task.retry_count < self.MAX_UPLOAD_RETRIES:
            # Exponential backoff: 1s, 2s, 4s
            backoff_time = 2 ** task.retry_count
            logger.warning(
                f"Upload failed for {task.local_path} (attempt {task.retry_count + 1}), "
                f"retrying in {backoff_time}s: {error}"
            )
            time.sleep(backoff_time)

            # Re-queue with incremented retry count
            retry_task = UploadTask(
                local_path=task.local_path,
                base_dir=task.base_dir,
                delete_after_upload=task.delete_after_upload,
                retry_count=task.retry_count + 1,
            )
            try:
                self._upload_queue.put(retry_task, timeout=5.0)
            except Exception:
                logger.error(f"Failed to re-queue upload for {task.local_path}")
        else:
            logger.error(
                f"Upload failed for {task.local_path} after {self.MAX_UPLOAD_RETRIES} retries, "
                f"file kept locally: {error}"
            )

    def _queue_upload(self, filepath: Path) -> None:
        """Queue a file for async upload to S3."""
        task = UploadTask(
            local_path=filepath,
            base_dir=self.output_dir,
            delete_after_upload=self.delete_local_after_upload,
            retry_count=0,
        )
        try:
            self._upload_queue.put(task, timeout=5.0)
            logger.debug(f"Queued upload for {filepath}")
        except Exception:
            # Queue full, fall back to synchronous upload
            logger.warning(f"Upload queue full, performing synchronous upload for {filepath}")
            success, s3_uri = self._s3_uploader.upload_file(
                local_path=filepath,
                base_dir=self.output_dir,
                delete_after_upload=self.delete_local_after_upload,
            )
            if not success:
                logger.warning(f"Synchronous S3 upload failed for {filepath}, file kept locally")

    def _wait_for_uploads(self, timeout: float = 60.0) -> None:
        """Wait for all pending uploads to complete."""
        if not self._upload_workers:
            return

        logger.info("Waiting for pending uploads to complete...")

        # Send shutdown sentinels to all workers
        for _ in self._upload_workers:
            try:
                self._upload_queue.put(None, timeout=5.0)
            except Exception:
                logger.warning("Failed to send shutdown sentinel to upload worker")

        # Wait for queue to drain
        start_time = time.time()
        while time.time() - start_time < timeout:
            with self._uploads_lock:
                in_flight = self._uploads_in_flight
            if self._upload_queue.empty() and in_flight == 0:
                break
            time.sleep(0.1)

        # Join worker threads
        for worker in self._upload_workers:
            worker.join(timeout=max(1.0, timeout - (time.time() - start_time)))

        if any(w.is_alive() for w in self._upload_workers):
            logger.warning("Some upload workers did not terminate in time")
        else:
            logger.info("All upload workers terminated")

    def flush(self) -> bool:
        """
        Flush all buffered records to parquet files.
        Returns True if successful, False otherwise.
        """
        with self.buffer_lock:
            if self.buffer_count == 0:
                return True

            # Deep copy buffer to avoid issues with nested defaultdicts
            buffer_snapshot = copy.deepcopy(self.buffer)
            pending_offsets = dict(self._pending_offsets)

            # Clear buffer early to allow new records while writing
            self.buffer = defaultdict(lambda: defaultdict(list))
            self.buffer_count = 0
            self._pending_offsets = {}

        try:
            files_written = []

            for date_key, symbols in buffer_snapshot.items():
                for symbol, records in symbols.items():
                    if records:
                        filepath = self._write_partition(date_key, symbol, records)
                        files_written.append(filepath)

            # Commit specific offsets per partition after successful write
            if pending_offsets and files_written:
                offsets_to_commit = [
                    TopicPartition(topic, partition, offset + 1)
                    for (topic, partition), offset in pending_offsets.items()
                ]
                self.consumer.commit(offsets=offsets_to_commit, asynchronous=False)
                logger.info(f"Committed offsets for {len(offsets_to_commit)} partitions after writing {len(files_written)} files")

            self._last_flush_time = time.time()
            return True

        except Exception as e:
            logger.error(f"Failed to flush buffer: {e}")
            # Re-add records to buffer for retry
            with self.buffer_lock:
                for date_key, symbols in buffer_snapshot.items():
                    for symbol, records in symbols.items():
                        self.buffer[date_key][symbol].extend(records)
                        self.buffer_count += len(records)
                # Merge back pending offsets (keep higher offset if conflict)
                for key, offset in pending_offsets.items():
                    if key not in self._pending_offsets or offset > self._pending_offsets[key]:
                        self._pending_offsets[key] = offset
            return False

    def _flush_loop(self) -> None:
        """Background thread for periodic flushing."""
        while self._running:
            time.sleep(1)
            if self._should_flush():
                self.flush()

    def _handle_message(self, msg) -> None:
        """Process a single Kafka message."""
        try:
            trade = self.deserialize(msg.value())
            self._add_to_buffer(trade)

            # Track latest offset per partition (protected by buffer_lock)
            with self.buffer_lock:
                key = (msg.topic(), msg.partition())
                current_offset = msg.offset()
                # Only keep the highest offset per partition
                if key not in self._pending_offsets or current_offset > self._pending_offsets[key]:
                    self._pending_offsets[key] = current_offset

        except Exception as e:
            logger.error(f"Failed to process message: {e}")

    def start(self, poll_timeout: float = 1.0) -> None:
        """Start consuming messages."""
        self._running = True
        self.consumer.subscribe([self.topic])

        # Start background flush thread
        self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._flush_thread.start()

        logger.info(f"Started consuming from topic: {self.topic}")

        try:
            while self._running:
                msg = self.consumer.poll(timeout=poll_timeout)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition {msg.partition()}")
                    else:
                        raise KafkaException(msg.error())
                else:
                    self._handle_message(msg)

        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        finally:
            self.stop()

    def stop(self) -> None:
        """Stop consumer and flush remaining data."""
        logger.info("Stopping consumer...")
        self._running = False

        # Final flush
        if self.buffer_count > 0:
            logger.info(f"Flushing {self.buffer_count} remaining records...")
            self.flush()

        # Wait for flush thread
        if self._flush_thread and self._flush_thread.is_alive():
            self._flush_thread.join(timeout=5)

        # Wait for pending uploads to complete
        if self.s3_enabled:
            self._wait_for_uploads()

        self.consumer.close()
        logger.info("Consumer stopped")


def main():
    """Main entry point."""

    load_dotenv()

    # Read configuration from environment with defaults
    s3_bucket = os.environ.get('S3_BUCKET')
    s3_prefix = os.environ.get('S3_PREFIX', 'trades')
    s3_region = os.environ.get('AWS_REGION')
    delete_local = os.environ.get('DELETE_LOCAL_AFTER_UPLOAD', 'false').lower() == 'true'

    consumer = TradeConsumer(
        bootstrap_servers='localhost:9092',
        topic='trades_topic',
        group_id='trade_consumer_group',
        schema_path='configs/trade_schema.avsc',
        output_dir='data/trades',
        flush_interval_seconds=30,
        # S3 configuration (None disables S3 upload)
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        s3_region=s3_region,
        delete_local_after_upload=delete_local,
    )

    if s3_bucket:
        logger.info(f"S3 upload configured: s3://{s3_bucket}/{s3_prefix}/")
    else:
        logger.info("S3 upload disabled (S3_BUCKET not set)")

    # Handle graceful shutdown
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}")
        consumer.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    consumer.start()


if __name__ == "__main__":
    main()
