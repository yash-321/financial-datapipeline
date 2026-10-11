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
	@echo "    make create-bucket       - Create landing bucket in Floci"
	@echo "    make list-buckets        - List buckets in Floci"
	@echo "    make list-objects        - List objects in landing bucket"
	@echo "    make setup-floci-iceberg - Setup Iceberg infrastructure (S3 + Glue databases)"
	@echo ""
	@echo "  Spark Jobs:"
	@echo "    make spark-bronze           - Run landing-to-bronze (full refresh)"
	@echo "    make spark-bronze-incremental - Run landing-to-bronze (append mode)"
	@echo "    make spark-bronze-floci     - Run landing-to-bronze with Floci (Glue catalog)"
	@echo "    make spark-silver-floci     - Run landing-to-silver with Floci (placeholder)"
	@echo "    make spark-gold-floci       - Run landing-to-gold with Floci (placeholder)"
	@echo "    make spark-bronze-dry       - Dry run (validate only)"
	@echo "    make spark-test-config      - Test Spark configuration"
	@echo "    make spark-test-utils       - Test Spark utilities"
	@echo "    make verify-floci-iceberg   - List Glue databases and tables"
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

# Setup Iceberg infrastructure for medallion architecture (S3 + Glue)
setup-floci-iceberg:
	@echo "Setting up Iceberg infrastructure in floci..."
	@echo ""
	@echo "Creating S3 buckets..."
	@aws s3 mb s3://landing-bucket 2>/dev/null || echo "  landing-bucket already exists"
	@aws s3 mb s3://iceberg-warehouse 2>/dev/null || echo "  iceberg-warehouse already exists"
	@echo ""
	@echo "Creating Glue databases for medallion layers..."
	@aws glue create-database --database-input '{"Name":"iceberg_bronze","Description":"Bronze: raw ingestion"}' 2>/dev/null || echo "  iceberg_bronze already exists"
	@aws glue create-database --database-input '{"Name":"iceberg_silver","Description":"Silver: cleaned/deduplicated"}' 2>/dev/null || echo "  iceberg_silver already exists"
	@aws glue create-database --database-input '{"Name":"iceberg_gold","Description":"Gold: analytics-ready"}' 2>/dev/null || echo "  iceberg_gold already exists"
	@echo ""
	@echo "✓ Iceberg infrastructure ready!"
	@echo ""
	@echo "Next steps:"
	@echo "  make spark-bronze-floci    # Run Spark job"
	@echo "  make verify-floci-iceberg  # Verify setup"

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
# Spark Jobs
# -----------------------------------------------------------------------------

# Run landing-to-bronze job locally (full refresh)
spark-bronze:
	@echo "Running Landing to Bronze Spark job..."
	cd spark/jobs && python landing_to_bronze.py --env local --mode overwrite

# Run landing-to-bronze job (incremental/append)
spark-bronze-incremental:
	@echo "Running Landing to Bronze Spark job (incremental)..."
	cd spark/jobs && python landing_to_bronze.py --env local --mode append

# Run landing-to-bronze job with floci (Glue catalog)
spark-bronze-floci:
	@echo "Running Landing to Bronze Spark job with Floci (Glue catalog)..."
	cd spark/jobs && python landing_to_bronze.py --env floci --layer bronze --mode overwrite

# Run landing-to-silver job with floci (placeholder)
spark-silver-floci:
	@echo "Running Landing to Silver Spark job with Floci (Glue catalog)..."
	@echo "NOTE: Silver job not yet implemented. Use bronze layer for now."
	cd spark/jobs && python landing_to_bronze.py --env floci --layer silver --mode overwrite

# Run landing-to-gold job with floci (placeholder)
spark-gold-floci:
	@echo "Running Landing to Gold Spark job with Floci (Glue catalog)..."
	@echo "NOTE: Gold job not yet implemented. Use bronze layer for now."
	cd spark/jobs && python landing_to_bronze.py --env floci --layer gold --mode overwrite

# Dry run (validate without writing)
spark-bronze-dry:
	@echo "Running Landing to Bronze Spark job (dry run)..."
	cd spark/jobs && python landing_to_bronze.py --env local --dry-run

# Test Spark config
spark-test-config:
	@echo "Testing Spark job configuration..."
	cd spark/jobs && python config.py

# Test Spark utilities
spark-test-utils:
	@echo "Testing Spark job utilities..."
	cd spark/jobs && python utils.py

# Verify Floci Iceberg setup (list Glue databases and tables)
verify-floci-iceberg:
	@echo "\nGlue Databases:"
	@aws glue get-databases --query 'DatabaseList[*].Name' --output table 2>/dev/null || echo "  (none or error)"
	@echo "\nTables in iceberg_bronze:"
	@aws glue get-tables --database-name iceberg_bronze --query 'TableList[*].Name' --output table 2>/dev/null || echo "  (none or error)"
	@echo "\nTables in iceberg_silver:"
	@aws glue get-tables --database-name iceberg_silver --query 'TableList[*].Name' --output table 2>/dev/null || echo "  (none or error)"
	@echo "\nTables in iceberg_gold:"
	@aws glue get-tables --database-name iceberg_gold --query 'TableList[*].Name' --output table 2>/dev/null || echo "  (none or error)"
	@echo "\nS3 Warehouse contents:"
	@aws s3 ls s3://iceberg-warehouse/ 2>/dev/null || echo "  (none or error)"

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
