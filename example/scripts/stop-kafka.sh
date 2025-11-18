#!/usr/bin/env bash
# Stop Kafka Docker Compose services

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXAMPLE_DIR="$(dirname "$SCRIPT_DIR")"

cd "$EXAMPLE_DIR"

echo "Stopping Kafka..."
docker compose down -v

echo "Kafka stopped and volumes removed."
