#!/usr/bin/env bash
# Playback recorded prices at normal speed (1x)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
EXAMPLE_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

# Find the most recent cassette directory
DATA_DIR="${EXAMPLE_DIR}/data"
CASSETTE=$(find "$DATA_DIR" -maxdepth 1 -type d -name "prices-*" | sort -r | head -n 1)

if [ -z "$CASSETTE" ]; then
  echo "ERROR: No cassette found in $DATA_DIR"
  echo "Please record data first using record-prices.sh"
  exit 1
fi

echo "Playing back cassette at 1x speed (normal speed):"
echo "  Cassette: $CASSETTE"
echo "  Target topic: prices_replay"
echo ""

java -jar build/libs/kcr-all.jar \
  --bootstrap-servers localhost:9092 \
  play \
  --cassette "$CASSETTE" \
  --topic prices_replay \
  --playback-rate 1.0
