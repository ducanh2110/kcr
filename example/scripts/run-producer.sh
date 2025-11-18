#!/usr/bin/env bash
# Run the price streaming producer

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

cd "$PROJECT_ROOT"

echo "Building example producer..."
./gradlew :example:build --no-daemon -q

echo ""
echo "Starting price stream producer..."
echo "Press Ctrl+C to stop"
echo ""

./gradlew :example:runProducer --no-daemon --console=plain
