FROM python:3.11-slim

# Build argument to select which consumer to run
ARG CONSUMER_MODULE=trades
ENV CONSUMER_MODULE=${CONSUMER_MODULE}

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy consumer source code
COPY src/ ./src/

# Copy configs (schemas mounted or copied in compose)
# Schemas are mounted at /app/configs/schemas/

# Run the specified consumer module
CMD ["sh", "-c", "python -m src.${CONSUMER_MODULE}"]
