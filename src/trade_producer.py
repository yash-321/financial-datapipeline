from confluent_kafka import Producer
from random import choice, uniform, random
import fastavro
import io
import json
import time
import uuid


class TradeProducer:
    """Kafka producer for trade events."""

    def __init__(
        self,
        bootstrap_servers: str = 'localhost:9092',
        topic: str = 'trades_topic',
        schema_path: str = 'configs/trade_schema.avsc',
    ):
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'acks': 'all'
        }
        self.producer = Producer(self.config)
        self.topic = topic
        self.symbols = ['AAPL', 'GOOG', 'MSFT', 'AMZN', 'TSLA']
        self.prices = {'AAPL': 150.0, 'GOOG': 2800.0, 'MSFT': 300.0, 'AMZN': 3500.0, 'TSLA': 700.0}

        self.schema = self._load_schema(schema_path)

    def _load_schema(self, schema_path: str) -> dict:
        """Load Avro schema from file."""
        with open(schema_path, 'r') as f:
            return json.load(f)

    def generate_trade(self) -> dict:
        """Generate a random trade event."""
        symbol = choice(self.symbols)
        self.prices[symbol] += uniform(-1.0, 1.0)
        price = self.prices[symbol]
        quantity = uniform(1.0, 500.0)
        event_timestamp = int(time.time() * 1000)

        if random() < uniform(0.05, 0.1):
            event_timestamp -= int(uniform(1, 120) * 1000)

        return {
            'symbol': symbol,
            'trade_id': str(uuid.uuid4()),
            'price': price,
            'quantity': quantity,
            'event_timestamp': event_timestamp,
            'ingestion_timestamp': None,  # Set at send time
        }

    def serialize(self, trade: dict) -> bytes:
        """Serialize trade data using Avro."""
        buffer = io.BytesIO()
        fastavro.schemaless_writer(buffer, self.schema, trade)
        return buffer.getvalue()

    def send(self, trade: dict, callback=None) -> None:
        """Send a trade to Kafka."""
        trade['ingestion_timestamp'] = int(time.time() * 1000)
        serialized = self.serialize(trade)

        def default_callback(err, msg):
            if err:
                print(f'ERROR: Message failed delivery: {err}')
            else:
                print(f"Produced event to topic {msg.topic()}: key = {msg.key().decode('utf-8')}")

        try:
            self.producer.produce(
                self.topic,
                key=trade['symbol'].encode(),
                value=serialized,
                callback=callback or default_callback
            )
        except BufferError as e:
            self.producer.poll(1)
            self.producer.produce(
                self.topic,
                key=trade['symbol'].encode(),
                value=serialized,
                callback=callback or default_callback
            )

    def flush(self) -> None:
        """Flush the producer to ensure all messages are sent."""
        self.producer.flush()


if __name__ == "__main__":
    producer = TradeProducer()

    for i in range(10000):
        trade = producer.generate_trade()
        producer.send(trade)
        print(f"[{i+1}/10000] Produced: {trade['symbol']} @ {trade['price']:.2f}")
        time.sleep(1 / 50)

    producer.flush()
