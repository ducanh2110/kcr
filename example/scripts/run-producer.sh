#!/usr/bin/env bash
# Run the price streaming producer

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

cd "$PROJECT_ROOT"

# Ensure we use Java 21
#export JAVA_HOME=/usr/lib/jvm/temurin-21-jdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

echo "Building example producer..."
./gradlew :example:build --no-daemon -q

echo ""
echo "Starting price stream producer..."
echo "Press Ctrl+C to stop"
echo ""

./gradlew :example:runProducer --no-daemon --console=plain
