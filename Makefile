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

.PHONY: help up up-floci up-floci-ui up-consumers down down-floci logs logs-floci-ui clean build produce

# Default target
help:
	@echo "Data Pipeline Commands:"
	@echo ""
	@echo "  Infrastructure:"
	@echo "    make up             - Start Kafka infrastructure (zookeeper, kafka, kafka-ui)"
	@echo "    make up-floci       - Start Floci AWS emulator + Floci UI"
	@echo "    make up-floci-ui    - Start Floci UI only (requires Floci running)"
	@echo "    make up-consumers   - Start Kafka + Floci + trade consumer"
	@echo "    make down           - Stop all services"
	@echo "    make down-floci     - Stop Floci + Floci UI services"
	@echo "    make restart        - Restart all services"
	@echo ""
	@echo "  Floci (AWS Emulator):"
	@echo "    make create-bucket  - Create landing bucket in Floci"
	@echo "    make list-buckets   - List buckets in Floci"
	@echo "    make list-objects   - List objects in landing bucket"
	@echo ""
	@echo "  Development:"
	@echo "    make build          - Build Docker images"
	@echo "    make logs           - Tail logs from all services"
	@echo "    make logs-kafka     - Tail Kafka logs"
	@echo "    make logs-consumer  - Tail consumer logs"
	@echo "    make logs-floci     - Tail Floci logs"
	@echo "    make logs-floci-ui  - Tail Floci UI logs"
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

# Start Floci AWS emulator + Floci UI
up-floci:
	docker compose --profile floci up -d floci floci-ui
	@echo "\n✓ Floci + Floci UI started"
	@echo "  Floci endpoint: http://localhost:4566"
	@echo "  Floci UI: http://localhost:4500"

# Start Floci UI only (requires Floci running)
up-floci-ui:
	docker compose --profile floci up -d floci-ui
	@echo "\n✓ Floci UI started"
	@echo "  Floci UI: http://localhost:4500"

# Start Kafka + consumers
up-consumers:
	docker compose --profile consumers up -d
	@echo "\n✓ Kafka + consumers started"
	@echo "  Kafka UI: http://localhost:8080"

# Stop all services
down:
	docker compose --profile consumers down

# Stop Floci + Floci UI services
down-floci:
	docker compose --profile floci stop floci-ui
	docker compose --profile floci rm -f floci-ui
	docker compose --profile floci stop floci
	docker compose --profile floci rm -f floci

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

logs-floci:
	docker compose --profile floci logs -f floci

logs-floci-ui:
	docker compose --profile floci logs -f floci-ui

# -----------------------------------------------------------------------------
# Floci (AWS Emulator)
# -----------------------------------------------------------------------------

# Create landing bucket in Floci
create-bucket:
	@docker run --rm --network datapipeline_network \
		-e AWS_ACCESS_KEY_ID=test \
		-e AWS_SECRET_ACCESS_KEY=test \
		-e AWS_DEFAULT_REGION=us-east-1 \
		amazon/aws-cli \
		--endpoint-url http://floci:4566 s3 mb s3://landing-bucket
	@echo "✓ Created landing-bucket in Floci"

# List buckets in Floci
list-buckets:
	@docker run --rm --network datapipeline_network \
		-e AWS_ACCESS_KEY_ID=test \
		-e AWS_SECRET_ACCESS_KEY=test \
		-e AWS_DEFAULT_REGION=us-east-1 \
		amazon/aws-cli \
		--endpoint-url http://floci:4566 s3 ls

# List objects in landing bucket
list-objects:
	@docker run --rm --network datapipeline_network \
		-e AWS_ACCESS_KEY_ID=test \
		-e AWS_SECRET_ACCESS_KEY=test \
		-e AWS_DEFAULT_REGION=us-east-1 \
		amazon/aws-cli \
		--endpoint-url http://floci:4566 s3 ls s3://landing-bucket/ --recursive

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
