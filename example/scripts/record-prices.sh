#!/usr/bin/env bash
# Record messages from the prices topic using kcr

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
EXAMPLE_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

# Ensure kcr is built
if [ ! -f "build/libs/kcr-all.jar" ]; then
  echo "Building kcr..."
  ./gradlew shadowJar --no-daemon -q
fi

DATA_DIR="${EXAMPLE_DIR}/data"
mkdir -p "$DATA_DIR"

echo "Recording from 'prices' topic..."
echo "Data will be saved to: $DATA_DIR"
echo "Press Ctrl+C to stop recording"
echo ""

java -jar build/libs/kcr-all.jar \
  --bootstrap-servers localhost:9092 \
  record \
  --topic prices \
  --data-directory "$DATA_DIR"
