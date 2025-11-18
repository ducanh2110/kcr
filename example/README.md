# KCR Example: Price Streaming Demo

This example demonstrates how to use Kafka Cassette Recorder (kcr) to record and replay Kafka messages with a realistic price streaming scenario.

## Features Demonstrated

- **Recording** messages from a Kafka topic
- **Playback** at different speeds (1x, 2x, 3x)
- **Multiple partitions** (3 partitions for better parallelism)
- **Realistic data** (JSON-formatted financial price data)

## Prerequisites

- **Docker** and **Docker Compose V2** (for running Kafka)
- **Java 21 JDK** (for running the producer and kcr) - The scripts automatically use Java 21
- **Bash** (for running the provided scripts)

**Note**: Java 21 is required. If you have multiple Java versions installed, the scripts will automatically use Java 21 from `/usr/lib/jvm/temurin-21-jdk-amd64`. If your Java 21 is in a different location, you can set `JAVA_HOME` before running the scripts.

## Quick Start

### 1. Start Kafka

Start a local Kafka broker using Docker Compose:

```bash
./scripts/start-kafka.sh
```

This will:
- Start a Kafka broker using Apache Kafka 3.7.0 (KRaft mode - no ZooKeeper needed)
- Create a `prices` topic with 3 partitions
- Make Kafka available at `localhost:9092`

### 2. Start the Price Producer

In a **new terminal**, start the price streaming producer:

```bash
./scripts/run-producer.sh
```

This Java application will continuously produce fake price data for financial securities (AAPL, MSFT, AMZN, etc.) to the `prices` topic at a rate of ~10 messages/second.

The messages are JSON-formatted with fields like:
```json
{
  "ticker": "AAPL",
  "price": 175.50,
  "volume": 450,
  "exchange": "NASDAQ",
  "timestamp": 1700312345678,
  "bid": 175.49,
  "ask": 175.51
}
```

### 3. Record Messages with kcr

In a **new terminal**, start recording messages:

```bash
./scripts/record-prices.sh
```

Let it run for 30-60 seconds to capture some data, then press **Ctrl+C** to stop.

The recorded data (cassette) will be saved in `./data/prices-<timestamp>/` directory with:
- One file per partition (`prices-0`, `prices-1`, `prices-2`)
- A manifest file with metadata
- Messages stored in JSON format with timestamp, key, value, headers, offset, and partition

### 4. Playback at Different Speeds

#### Normal Speed (1x)

Replay the recorded messages at the same rate they were captured:

```bash
./scripts/play-prices-1x.sh
```

Messages will be sent to the `prices_replay` topic at the original recording rate.

#### 2x Speed

Replay at twice the speed:

```bash
./scripts/play-prices-2x.sh
```

If messages were recorded at 10 msg/sec, they'll be replayed at 20 msg/sec.

#### 3x Speed

Replay at three times the speed:

```bash
./scripts/play-prices-3x.sh
```

### 5. View Messages (Optional)

To view messages being produced or replayed:

```bash
# View original prices topic
./scripts/consume-prices.sh prices

# View replayed messages
./scripts/consume-prices.sh prices_replay
```

### 6. Cleanup

Stop the producer (Ctrl+C in the producer terminal) and stop Kafka:

```bash
./scripts/stop-kafka.sh
```

This removes the Docker containers and volumes.

## Directory Structure

```
example/
├── README.md                          # This file
├── docker-compose.yml                 # Kafka setup using Bitnami images
├── build.gradle                       # Gradle build for the example module
├── scripts/                           # Helper scripts
│   ├── start-kafka.sh                # Start Kafka with Docker
│   ├── stop-kafka.sh                 # Stop Kafka
│   ├── run-producer.sh               # Run the price producer
│   ├── record-prices.sh              # Record with kcr
│   ├── play-prices-1x.sh             # Playback at 1x speed
│   ├── play-prices-2x.sh             # Playback at 2x speed
│   ├── play-prices-3x.sh             # Playback at 3x speed
│   └── consume-prices.sh             # View messages in a topic
├── src/main/java/                     # Java source code
│   └── com/nordstrom/kafka/kcr/example/
│       └── PriceStreamProducer.java   # Price data generator
└── data/                              # Recorded cassettes (git-ignored)
    └── prices-<timestamp>/            # Cassette directory
        ├── manifest.json              # Metadata
        ├── prices-0                   # Partition 0 messages
        ├── prices-1                   # Partition 1 messages
        └── prices-2                   # Partition 2 messages
```

## Understanding Variable-Speed Playback

kcr preserves the **relative timing** between messages during playback:

- **1x speed**: Messages are replayed with the exact same time intervals as when they were recorded
- **2x speed**: Time intervals are halved (messages arrive twice as fast)
- **3x speed**: Time intervals are divided by 3 (three times as fast)

For example, if two messages were recorded 100ms apart:
- At 1x speed: They'll be replayed 100ms apart
- At 2x speed: They'll be replayed 50ms apart
- At 3x speed: They'll be replayed ~33ms apart

This is useful for:
- **Load testing** (replay at higher speeds to stress-test consumers)
- **Debugging** (replay at slower speeds to analyze behavior)
- **Time travel** (replay production data in test environments)

## Customizing the Producer

You can customize the producer behavior with environment variables:

```bash
# Change the message rate (in milliseconds between messages)
RATE_MS=50 ./scripts/run-producer.sh  # 20 messages/second

# Use a different topic
TOPIC=my-prices ./scripts/run-producer.sh

# Use a different Kafka broker
BOOTSTRAP_SERVERS=kafka.example.com:9092 ./scripts/run-producer.sh
```

## Troubleshooting

### Kafka won't start
- Make sure Docker is running
- Check if port 9092 is already in use: `lsof -i :9092`
- Try stopping and starting again: `./scripts/stop-kafka.sh && ./scripts/start-kafka.sh`

### Producer fails to connect
- Ensure Kafka is running: `docker ps | grep kcr-kafka`
- Check Kafka logs: `docker logs kcr-kafka`
- Verify the topic exists: `docker exec kcr-kafka kafka-topics.sh --bootstrap-server localhost:9092 --list`

### No cassette found for playback
- Make sure you've run `./scripts/record-prices.sh` and let it capture data
- Check that the cassette directory exists: `ls -la data/`

### Build failures
- Ensure you have Java 21: `java -version`
- Try cleaning and rebuilding: `cd .. && ./gradlew clean build`

## Next Steps

- Explore the recorded cassette format by examining files in `data/prices-*/`
- Try recording with the `--duration` flag: `java -jar ../build/libs/kcr-all.jar record --topic prices --duration 0h1m0s`
- Experiment with different playback rates
- Try recording from and replaying to different topics
- Modify the producer to generate different types of data

## Additional Resources

- [Main kcr README](../README.md) - Full documentation
- [kcr CLI help](../README.md#usage) - All command-line options
- [Kafka Documentation](https://kafka.apache.org/documentation/) - Apache Kafka docs
