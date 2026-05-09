"""Trades producer - generates and sends trade events to Kafka.

This producer:
1. Generates synthetic trade events with realistic price movements
2. Serializes with Avro schema
3. Produces to Kafka trades_topic
4. Optionally introduces duplicates, corrections, and late events for testing
"""

from random import choice, randint, uniform, random
from typing import Optional
import os
import time
import uuid

from dotenv import load_dotenv

from .base import BaseProducer, ProducerConfig


class TradesProducer(BaseProducer):
    """Producer for synthetic trade events."""

    # Stock symbols and starting prices
    DEFAULT_SYMBOLS = ['AAPL', 'GOOG', 'MSFT', 'AMZN', 'TSLA']
    DEFAULT_PRICES = {
        'AAPL': 150.0,
        'GOOG': 2800.0,
        'MSFT': 300.0,
        'AMZN': 3500.0,
        'TSLA': 700.0,
    }

    def __init__(
        self,
        config: ProducerConfig,
        symbols: Optional[list[str]] = None,
        initial_prices: Optional[dict[str, float]] = None,
        # Testing options
        duplicate_rate: float = 0.05,
        correction_rate: float = 0.05,
        late_event_rate: float = 0.02,
    ):
        """Initialize trades producer.
        
        Args:
            config: Producer configuration
            symbols: List of stock symbols to generate trades for
            initial_prices: Initial prices for each symbol
            duplicate_rate: Probability of sending duplicate trade
            correction_rate: Probability of sending price correction
            late_event_rate: Probability of sending late (backdated) event
        """
        super().__init__(config)
        
        self.symbols = symbols or self.DEFAULT_SYMBOLS
        self.prices = dict(initial_prices or self.DEFAULT_PRICES)
        
        # Testing rates
        self.duplicate_rate = duplicate_rate
        self.correction_rate = correction_rate
        self.late_event_rate = late_event_rate
        
        # Counters for test events
        self.duplicate_count = 0
        self.correction_count = 0
        self.late_event_count = 0

    def get_key(self, record: dict) -> str:
        """Use symbol as partition key for ordering by symbol."""
        return record['symbol']

    def generate(self) -> dict:
        """Generate a random trade event with realistic price movement."""
        symbol = choice(self.symbols)
        
        # Random walk price movement
        self.prices[symbol] += uniform(-1.0, 1.0)
        self.prices[symbol] = max(0.01, self.prices[symbol])  # Prevent negative
        
        price = self.prices[symbol]
        quantity = uniform(1.0, 500.0)
        event_timestamp = int(time.time() * 1000)

        # Occasionally introduce late events (for testing out-of-order handling)
        if random() < self.late_event_rate:
            event_timestamp -= int(uniform(1, 120) * 1000)  # 1-120 seconds late

        return {
            'symbol': symbol,
            'trade_id': str(uuid.uuid4()),
            'price': round(price, 2),
            'quantity': round(quantity, 4),
            'event_timestamp': event_timestamp,
            'ingestion_timestamp': None,  # Set at send time
        }

    def transform_record(self, record: dict) -> dict:
        """Add ingestion timestamp before sending."""
        record['ingestion_timestamp'] = int(time.time() * 1000)
        return record

    def send_with_test_events(self, record: dict) -> dict:
        """Send a record and optionally send test events (duplicates, corrections).
        
        Returns dict with counts of additional events sent.
        """
        counts = {'duplicate': 0, 'corrected': 0, 'late': 0}

        # Maybe send duplicate
        if random() < self.duplicate_rate:
            self.send(record)
            counts['duplicate'] += 1
            self.duplicate_count += 1

        # Maybe send correction (same trade_id, slightly different price)
        if random() < self.correction_rate:
            corrected = record.copy()
            corrected['price'] = round(record['price'] + uniform(-0.5, 0.5), 2)
            corrected['event_timestamp'] += randint(1, 5000)
            self.send(corrected)
            counts['corrected'] += 1
            self.correction_count += 1

        # Maybe send older version of event
        if random() < self.late_event_rate:
            older = record.copy()
            older['event_timestamp'] -= 10000
            older['ingestion_timestamp'] = int(time.time() * 1000)
            self.send(older)
            counts['late'] += 1
            self.late_event_count += 1

        return counts

    def produce_batch_with_test_events(
        self,
        count: int,
        delay_seconds: float = 0.02,
        poll_interval: int = 100,
    ) -> dict:
        """Produce a batch with test events (duplicates, corrections, late).
        
        Returns dict with statistics including test event counts.
        """
        self.stats.reset()
        self.duplicate_count = 0
        self.correction_count = 0
        self.late_event_count = 0
        
        for i in range(count):
            record = self.generate()
            self.send(record)
            self.send_with_test_events(record)
            
            if (i + 1) % poll_interval == 0:
                self.poll(0)
                
            if delay_seconds > 0:
                time.sleep(delay_seconds)
            
            if (i + 1) % 1000 == 0:
                self.logger.info(f"Produced {i + 1}/{count} messages")

        self.flush()
        
        total = count + self.duplicate_count + self.correction_count + self.late_event_count
        
        self.logger.info(
            f"Batch complete: {count} trades + {self.duplicate_count} duplicates, "
            f"{self.correction_count} corrections, {self.late_event_count} late = {total} total"
        )
        
        return {
            'trades': count,
            'duplicates': self.duplicate_count,
            'corrections': self.correction_count,
            'late_events': self.late_event_count,
            'total': total,
            'sent': self.stats.messages_sent,
            'failed': self.stats.messages_failed,
        }


def main():
    """Main entry point for trades producer."""
    load_dotenv()

    config = ProducerConfig(
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        topic=os.getenv('TRADES_TOPIC', 'trades_topic'),
        schema_path=os.getenv('TRADES_SCHEMA_PATH', 'configs/schemas/trade_schema.avsc'),
    )

    producer = TradesProducer(config)

    # Default: produce 10,000 trades
    count = int(os.getenv('TRADE_COUNT', '10000'))
    delay = float(os.getenv('TRADE_DELAY', '0.02'))  # 50 trades/sec

    producer.logger.info(f"Producing {count} trades to {config.topic}")
    
    stats = producer.produce_batch_with_test_events(
        count=count,
        delay_seconds=delay,
    )
    
    print(f"\nProduction complete:")
    print(f"  Trades: {stats['trades']}")
    print(f"  Duplicates: {stats['duplicates']}")
    print(f"  Corrections: {stats['corrections']}")
    print(f"  Late events: {stats['late_events']}")
    print(f"  Total sent: {stats['total']}")


if __name__ == "__main__":
    main()
