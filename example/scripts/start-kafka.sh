#!/usr/bin/env bash
# Start Kafka using Docker Compose

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXAMPLE_DIR="$(dirname "$SCRIPT_DIR")"

cd "$EXAMPLE_DIR"

echo "Starting Kafka with Docker Compose..."
docker-compose up -d

echo "Waiting for Kafka to be ready..."
sleep 10

# Wait for Kafka to be healthy
for i in {1..30}; do
  if docker exec kcr-kafka kafka-topics.sh --bootstrap-server localhost:9092 --list &>/dev/null; then
    echo "Kafka is ready!"
    
    # Create the prices topic with 3 partitions
    echo "Creating 'prices' topic with 3 partitions..."
    docker exec kcr-kafka kafka-topics.sh --bootstrap-server localhost:9092 \
      --create --if-not-exists --topic prices --partitions 3 --replication-factor 1
    
    echo ""
    echo "Kafka is running and ready!"
    echo "Bootstrap servers: localhost:9092"
    echo "Topic 'prices' created with 3 partitions"
    exit 0
  fi
  echo "Waiting for Kafka... ($i/30)"
  sleep 2
done

echo "ERROR: Kafka did not become ready in time"
exit 1
