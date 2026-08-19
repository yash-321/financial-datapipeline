"""Base consumer class for Kafka topic consumers.

Extend this class to create consumers for different topics (trades, accounts, etc.).
Handles common functionality: Kafka connection, Avro deserialization, batching,
error handling, and graceful shutdown.
"""

from abc import ABC, abstractmethod
from collections import defaultdict
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
import copy
import fastavro
import io
import json
import logging
import os
import signal
import threading
import time

from dotenv import load_dotenv


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


@dataclass
class ConsumerConfig:
    """Configuration for a Kafka consumer."""
    bootstrap_servers: str = 'localhost:9092'
    topic: str = ''
    group_id: str = ''
    schema_path: str = ''
    output_dir: str = ''
    flush_interval_seconds: int = 30
    max_buffer_size: int = 100_000
    auto_offset_reset: str = 'earliest'

    @classmethod
    def from_env(cls, prefix: str = '') -> 'ConsumerConfig':
        """Load configuration from environment variables with optional prefix."""
        p = f"{prefix}_" if prefix else ""
        return cls(
            bootstrap_servers=os.getenv(f'{p}KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            topic=os.getenv(f'{p}KAFKA_TOPIC', ''),
            group_id=os.getenv(f'{p}KAFKA_GROUP_ID', ''),
            schema_path=os.getenv(f'{p}SCHEMA_PATH', ''),
            output_dir=os.getenv(f'{p}OUTPUT_DIR', ''),
            flush_interval_seconds=int(os.getenv(f'{p}FLUSH_INTERVAL_SECONDS', '20')),
            max_buffer_size=int(os.getenv(f'{p}MAX_BUFFER_SIZE', '100000')),
            auto_offset_reset=os.getenv(f'{p}AUTO_OFFSET_RESET', 'earliest'),
        )


class BaseConsumer(ABC):
    """Abstract base class for Kafka consumers.
    
    Subclasses must implement:
    - process_batch(): Topic-specific batch processing logic
    - get_partition_keys(): Extract partition keys from a record for output organization
    
    Optional overrides:
    - transform_record(): Transform deserialized record before buffering
    - get_arrow_schema(): Define PyArrow schema for parquet output
    """

    def __init__(self, config: ConsumerConfig):
        """Initialize the consumer with configuration.
        
        Args:
            config: ConsumerConfig instance with connection and processing settings
        """
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Kafka consumer setup
        self._kafka_config = {
            'bootstrap.servers': config.bootstrap_servers,
            'group.id': config.group_id,
            'auto.offset.reset': config.auto_offset_reset,
            'enable.auto.commit': False,  # Manual commit for at-least-once semantics
        }
        self.consumer = Consumer(self._kafka_config)
        
        # Output directory
        self.output_dir = Path(config.output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load Avro schema
        self.schema = self._load_schema(config.schema_path)
        
        # Buffer for batching: partition_key -> list of records
        self.buffer: dict[str, list[dict]] = defaultdict(list)
        self.buffer_lock = threading.Lock()
        self.buffer_count = 0
        
        # Offset tracking for manual commits
        self._pending_offsets: dict[tuple[str, int], int] = {}
        
        # Control flags
        self._running = False
        self._flush_thread: Optional[threading.Thread] = None
        self._last_flush_time = time.time()

    def _load_schema(self, schema_path: str) -> dict:
        """Load Avro schema from file."""
        with open(schema_path, 'r') as f:
            return json.load(f)

    def deserialize(self, data: bytes) -> dict:
        """Deserialize Avro-encoded message."""
        buffer = io.BytesIO(data)
        return fastavro.schemaless_reader(buffer, self.schema)

    def transform_record(self, record: dict) -> dict:
        """Transform a deserialized record before buffering.
        
        Override in subclass for topic-specific transformations.
        Default implementation returns record unchanged.
        """
        return record

    @abstractmethod
    def get_partition_keys(self, record: dict) -> tuple[str, ...]:
        """Extract partition keys from a record for organizing output.
        
        Returns a tuple of key values used to construct the output path.
        Example: ('2026-05-01', 'AAPL') for date and symbol partitioning.
        """
        pass

    @abstractmethod
    def process_batch(self, partition_key: str, records: list[dict]) -> Path:
        """Process a batch of records for a given partition key.
        
        Args:
            partition_key: String representation of partition (e.g., 'date=2026-05-01/symbol=AAPL')
            records: List of records to process
            
        Returns:
            Path to the output file created
        """
        pass

    def _build_partition_key(self, record: dict) -> str:
        """Build a string partition key from record's partition values."""
        keys = self.get_partition_keys(record)
        return '/'.join(str(k) for k in keys)

    def _add_to_buffer(self, record: dict) -> None:
        """Add a transformed record to the buffer."""
        partition_key = self._build_partition_key(record)
        
        with self.buffer_lock:
            self.buffer[partition_key].append(record)
            self.buffer_count += 1

    def _should_flush(self) -> bool:
        """Check if buffer should be flushed based on time or size."""
        time_exceeded = (time.time() - self._last_flush_time) >= self.config.flush_interval_seconds
        size_exceeded = self.buffer_count >= self.config.max_buffer_size
        return (time_exceeded or size_exceeded) and self.buffer_count > 0

    def flush(self) -> bool:
        """Flush all buffered records by calling process_batch for each partition."""
        with self.buffer_lock:
            if self.buffer_count == 0:
                return True

            buffer_snapshot = dict(self.buffer)
            pending_offsets = dict(self._pending_offsets)
            
            # Clear buffer to allow new records during processing
            self.buffer = defaultdict(list)
            self.buffer_count = 0
            self._pending_offsets = {}

        try:
            files_written = []
            
            for partition_key, records in buffer_snapshot.items():
                if records:
                    filepath = self.process_batch(partition_key, records)
                    files_written.append(filepath)
                    self.logger.info(f"Processed {len(records)} records -> {filepath}")

            # Commit offsets after successful processing
            if pending_offsets and files_written:
                offsets_to_commit = [
                    TopicPartition(topic, partition, offset + 1)
                    for (topic, partition), offset in pending_offsets.items()
                ]
                self.consumer.commit(offsets=offsets_to_commit, asynchronous=False)
                self.logger.info(
                    f"Committed offsets for {len(offsets_to_commit)} partitions "
                    f"after writing {len(files_written)} files"
                )

            self._last_flush_time = time.time()
            return True

        except Exception as e:
            self.logger.error(f"Failed to flush buffer: {e}")
            # Re-add records to buffer for retry
            with self.buffer_lock:
                for partition_key, records in buffer_snapshot.items():
                    self.buffer[partition_key].extend(records)
                    self.buffer_count += len(records)
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
            record = self.deserialize(msg.value())
            record = self.transform_record(record)
            self._add_to_buffer(record)

            # Track latest offset per partition
            with self.buffer_lock:
                key = (msg.topic(), msg.partition())
                current_offset = msg.offset()
                if key not in self._pending_offsets or current_offset > self._pending_offsets[key]:
                    self._pending_offsets[key] = current_offset

        except Exception as e:
            self.logger.error(f"Failed to process message: {e}")

    def start(self, poll_timeout: float = 1.0) -> None:
        """Start consuming messages from Kafka."""
        self._running = True
        self.consumer.subscribe([self.config.topic])

        # Start background flush thread
        self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._flush_thread.start()

        self.logger.info(f"Started consuming from topic: {self.config.topic}")

        try:
            while self._running:
                msg = self.consumer.poll(timeout=poll_timeout)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        self.logger.debug(f"Reached end of partition {msg.partition()}")
                    else:
                        raise KafkaException(msg.error())
                else:
                    self._handle_message(msg)

        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal")
        finally:
            self.stop()

    def stop(self) -> None:
        """Stop consumer and flush remaining data."""
        self.logger.info("Stopping consumer...")
        self._running = False

        # Final flush
        if self.buffer_count > 0:
            self.logger.info(f"Flushing {self.buffer_count} remaining records...")
            self.flush()

        # Wait for flush thread
        if self._flush_thread and self._flush_thread.is_alive():
            self._flush_thread.join(timeout=5)

        self.consumer.close()
        self.logger.info("Consumer stopped")


def setup_signal_handlers(consumer: BaseConsumer) -> None:
    """Setup graceful shutdown signal handlers."""
    def signal_handler(signum, frame):
        consumer.logger.info(f"Received signal {signum}")
        consumer.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
