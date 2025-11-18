#!/usr/bin/env bash
# Consume messages from prices or prices_replay topic to view the data

set -e

TOPIC=${1:-prices}

echo "Consuming from topic: $TOPIC"
echo "Press Ctrl+C to stop"
echo ""

docker exec -it kcr-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic "$TOPIC" \
  --from-beginning \
  --property print.key=true \
  --property print.timestamp=true \
  --property key.separator=" | "
