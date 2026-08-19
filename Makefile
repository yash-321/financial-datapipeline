# =============================================================================
# Data Pipeline Makefile
# =============================================================================
# Quick reference:
#   make up           - Start Kafka infrastructure
#   make up-consumers - Start Kafka + consumer
#   make down         - Stop all services
#   make logs         - Tail all logs
#   make produce      - Run trade producer locally
# =============================================================================

.PHONY: help up up-consumers down logs clean build produce

# Default target
help:
	@echo "Data Pipeline Commands:"
	@echo ""
	@echo "  Infrastructure:"
	@echo "    make up             - Start Kafka infrastructure (zookeeper, kafka, kafka-ui)"
	@echo "    make up-consumers   - Start Kafka + trade consumer"
	@echo "    make down           - Stop all services"
	@echo "    make restart        - Restart all services"
	@echo ""
	@echo "  Development:"
	@echo "    make build          - Build Docker images"
	@echo "    make logs           - Tail logs from all services"
	@echo "    make logs-kafka     - Tail Kafka logs"
	@echo "    make logs-consumer  - Tail consumer logs"
	@echo ""
	@echo "  Data Production:"
	@echo "    make produce        - Run trade producer (local, requires venv)"
	@echo "    make produce-docker - Run trade producer in Docker"
	@echo ""
	@echo "  Utilities:"
	@echo "    make ps             - Show running containers"
	@echo "    make clean          - Remove containers, volumes, and build cache"
	@echo "    make topics         - List Kafka topics"
	@echo "    make shell-kafka    - Open shell in Kafka container"

# -----------------------------------------------------------------------------
# Infrastructure
# -----------------------------------------------------------------------------

# Start Kafka infrastructure only
up:
	docker compose up -d
	@echo "\n✓ Kafka infrastructure started"
	@echo "  Kafka UI: http://localhost:8080"

# Start Kafka + consumers
up-consumers:
	docker compose --profile consumers up -d
	@echo "\n✓ Kafka + consumers started"
	@echo "  Kafka UI: http://localhost:8080"

# Stop all services
down:
	docker compose --profile consumers down

# Restart services
restart: down up

# -----------------------------------------------------------------------------
# Building
# -----------------------------------------------------------------------------

# Build all images
build:
	docker compose --profile consumers build

# Build consumer image only
build-consumer:
	docker compose build trade-consumer

# -----------------------------------------------------------------------------
# Logs
# -----------------------------------------------------------------------------

# Tail all logs
logs:
	docker compose --profile consumers logs -f

# Tail specific service logs
logs-kafka:
	docker compose logs -f kafka

logs-consumer:
	docker compose --profile consumers logs -f trade-consumer

# -----------------------------------------------------------------------------
# Data Production
# -----------------------------------------------------------------------------

# Run producer locally (requires local Python environment)
produce:
	@echo "Running trade producer..."
	cd producers && python -m src.trades

# Run producer in a one-off container
produce-docker:
	docker run --rm \
		--network datapipeline_network \
		-v $(PWD)/configs/schemas:/app/configs/schemas:ro \
		-e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
		-e TRADES_TOPIC=trades_topic \
		-e TRADES_SCHEMA_PATH=/app/configs/schemas/trade_schema.avsc \
		-e TRADE_COUNT=1000 \
		python:3.11-slim \
		bash -c "pip install -q confluent-kafka fastavro python-dotenv && \
		         cd /app && python -m producers.src.trades"

# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------

# Show running containers
ps:
	docker compose --profile consumers ps

# List Kafka topics
topics:
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Open shell in Kafka container
shell-kafka:
	docker exec -it kafka bash

# Open shell in consumer container
shell-consumer:
	docker exec -it trade-consumer bash

# -----------------------------------------------------------------------------
# Cleanup
# -----------------------------------------------------------------------------

# Full cleanup - remove containers, volumes, and build cache
clean:
	docker compose --profile consumers down -v --rmi local
	docker system prune -f
	@echo "✓ Cleaned up containers, volumes, and build cache"

# Remove only volumes (data)
clean-data:
	docker compose --profile consumers down -v
	rm -rf data/trades/*
	@echo "✓ Removed data volumes"
