"""Base producer class for Kafka topic producers.

Extend this class to create producers for different topics (trades, accounts, etc.).
Handles common functionality: Kafka connection, Avro serialization, batching,
delivery callbacks, and graceful shutdown.
"""

from abc import ABC, abstractmethod
from confluent_kafka import Producer
from dataclasses import dataclass
from typing import Any, Callable, Optional
import fastavro
import io
import json
import logging
import os
import time

from dotenv import load_dotenv


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


@dataclass
class ProducerConfig:
    """Configuration for a Kafka producer."""
    bootstrap_servers: str = 'localhost:9092'
    topic: str = ''
    schema_path: str = ''
    acks: str = 'all'
    batch_size: int = 16384
    linger_ms: int = 5
    
    @classmethod
    def from_env(cls, prefix: str = '') -> 'ProducerConfig':
        """Load configuration from environment variables with optional prefix."""
        p = f"{prefix}_" if prefix else ""
        return cls(
            bootstrap_servers=os.getenv(f'{p}KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            topic=os.getenv(f'{p}KAFKA_TOPIC', ''),
            schema_path=os.getenv(f'{p}SCHEMA_PATH', ''),
            acks=os.getenv(f'{p}KAFKA_ACKS', 'all'),
            batch_size=int(os.getenv(f'{p}KAFKA_BATCH_SIZE', '16384')),
            linger_ms=int(os.getenv(f'{p}KAFKA_LINGER_MS', '5')),
        )


@dataclass
class ProducerStats:
    """Statistics for producer operations."""
    messages_sent: int = 0
    messages_failed: int = 0
    bytes_sent: int = 0
    
    def reset(self) -> None:
        self.messages_sent = 0
        self.messages_failed = 0
        self.bytes_sent = 0


class BaseProducer(ABC):
    """Abstract base class for Kafka producers.
    
    Subclasses must implement:
    - generate(): Generate a single message/record
    - get_key(): Extract the partition key from a record
    
    Optional overrides:
    - on_delivery(): Custom delivery callback
    - transform_record(): Transform record before serialization
    """

    def __init__(self, config: ProducerConfig):
        """Initialize the producer with configuration.
        
        Args:
            config: ProducerConfig instance with connection settings
        """
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Kafka producer setup
        self._kafka_config = {
            'bootstrap.servers': config.bootstrap_servers,
            'acks': config.acks,
            'batch.size': config.batch_size,
            'linger.ms': config.linger_ms,
        }
        self.producer = Producer(self._kafka_config)
        
        # Load Avro schema
        self.schema = self._load_schema(config.schema_path)
        
        # Statistics
        self.stats = ProducerStats()

    def _load_schema(self, schema_path: str) -> dict:
        """Load Avro schema from file."""
        with open(schema_path, 'r') as f:
            return json.load(f)

    def serialize(self, record: dict) -> bytes:
        """Serialize record using Avro schema."""
        buffer = io.BytesIO()
        fastavro.schemaless_writer(buffer, self.schema, record)
        return buffer.getvalue()

    @abstractmethod
    def generate(self) -> dict:
        """Generate a single message/record.
        
        Returns:
            Dictionary representing the record to produce
        """
        pass

    @abstractmethod
    def get_key(self, record: dict) -> str:
        """Extract the partition key from a record.
        
        Args:
            record: The record being produced
            
        Returns:
            String key for Kafka partitioning
        """
        pass

    def transform_record(self, record: dict) -> dict:
        """Transform a record before serialization.
        
        Override in subclass for topic-specific transformations.
        Default implementation returns record unchanged.
        """
        return record

    def on_delivery(self, err, msg) -> None:
        """Callback invoked on message delivery.
        
        Override in subclass for custom delivery handling.
        
        Args:
            err: Error object if delivery failed, None otherwise
            msg: The delivered message
        """
        if err:
            self.logger.error(f"Message delivery failed: {err}")
            self.stats.messages_failed += 1
        else:
            self.stats.messages_sent += 1
            self.stats.bytes_sent += len(msg.value())
            self.logger.debug(
                f"Delivered to {msg.topic()}[{msg.partition()}] @ offset {msg.offset()}"
            )

    def send(
        self,
        record: dict,
        callback: Optional[Callable] = None,
    ) -> None:
        """Send a record to Kafka.
        
        Args:
            record: Dictionary record to send
            callback: Optional custom delivery callback
        """
        # Transform and serialize
        record = self.transform_record(record)
        key = self.get_key(record)
        value = self.serialize(record)

        try:
            self.producer.produce(
                self.config.topic,
                key=key.encode('utf-8'),
                value=value,
                callback=callback or self.on_delivery,
            )
        except BufferError:
            # Buffer full - poll to make room and retry
            self.logger.warning("Producer buffer full, polling...")
            self.producer.poll(1)
            self.producer.produce(
                self.config.topic,
                key=key.encode('utf-8'),
                value=value,
                callback=callback or self.on_delivery,
            )

    def flush(self, timeout: float = 10.0) -> int:
        """Flush all buffered messages.
        
        Args:
            timeout: Maximum time to wait for flush
            
        Returns:
            Number of messages still in queue (0 if all flushed)
        """
        remaining = self.producer.flush(timeout)
        if remaining > 0:
            self.logger.warning(f"{remaining} messages still in queue after flush")
        return remaining

    def poll(self, timeout: float = 0) -> int:
        """Poll for delivery callbacks.
        
        Args:
            timeout: Maximum time to block
            
        Returns:
            Number of callbacks processed
        """
        return self.producer.poll(timeout)

    def produce_batch(
        self,
        count: int,
        delay_seconds: float = 0,
        poll_interval: int = 100,
    ) -> ProducerStats:
        """Generate and produce a batch of messages.
        
        Args:
            count: Number of messages to produce
            delay_seconds: Delay between messages (for rate limiting)
            poll_interval: Poll for callbacks every N messages
            
        Returns:
            ProducerStats with batch statistics
        """
        self.stats.reset()
        
        for i in range(count):
            record = self.generate()
            self.send(record)
            
            # Periodic poll to trigger callbacks and prevent buffer overflow
            if (i + 1) % poll_interval == 0:
                self.poll(0)
                
            if delay_seconds > 0:
                time.sleep(delay_seconds)
            
            if (i + 1) % 1000 == 0:
                self.logger.info(f"Produced {i + 1}/{count} messages")

        # Final flush
        self.flush()
        
        self.logger.info(
            f"Batch complete: {self.stats.messages_sent} sent, "
            f"{self.stats.messages_failed} failed, "
            f"{self.stats.bytes_sent / 1024:.2f} KB"
        )
        
        return self.stats
