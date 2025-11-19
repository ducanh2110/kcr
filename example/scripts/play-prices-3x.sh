#!/usr/bin/env bash
# Playback recorded prices at 3x speed

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
EXAMPLE_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

# Ensure we use Java 21
#export JAVA_HOME=/usr/lib/jvm/temurin-21-jdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

# Find the most recent cassette directory
DATA_DIR="${EXAMPLE_DIR}/data"
CASSETTE=$(find "$DATA_DIR" -maxdepth 1 -type d -name "kcr-prices-*" | sort -r | head -n 1)

if [ -z "$CASSETTE" ]; then
  echo "ERROR: No cassette found in $DATA_DIR"
  echo "Please record data first using record-prices.sh"
  exit 1
fi

echo "Playing back cassette at 3x speed (three times as fast):"
echo "  Cassette: $CASSETTE"
echo "  Target topic: prices_replay"
echo ""

java -jar build/libs/kcr-all.jar \
  --bootstrap-servers localhost:9092 \
  play \
  --cassette "$CASSETTE" \
  --topic prices_replay \
  --playback-rate 100.0
