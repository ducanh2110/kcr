package com.nordstrom.kafka.kcr.kafka;

import com.nordstrom.kafka.kcr.io.Source;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSource implements Source {
  private static final Logger log = LoggerFactory.getLogger(KafkaSource.class);

  public static final String BYTE_ARRAY_DESERIALIZER =
      "org.apache.kafka.common.serialization.ByteArrayDeserializer";
  public static final String BYTE_ARRAY_SERIALIZER =
      "org.apache.kafka.common.serialization.ByteArraySerializer";
  public static final String STRING_DESERIALIZER =
      "org.apache.kafka.common.serialization.StringDeserializer";
  public static final String STRING_SERIALIZER =
      "org.apache.kafka.common.serialization.StringSerializer";

  private final KafkaConsumer<byte[], byte[]> client;
  private final String topic;
  private final int partitionNumber;

  public KafkaSource(Properties config, String topic, int partitionNumber) {
    this.topic = topic;
    this.partitionNumber = partitionNumber;

    Properties consumerConfig = new Properties();
    consumerConfig.putAll(config);
    consumerConfig.put("key.deserializer", BYTE_ARRAY_DESERIALIZER);
    consumerConfig.put("value.deserializer", BYTE_ARRAY_DESERIALIZER);
    consumerConfig.put("enable.auto.commit", "true");
    consumerConfig.put("auto.offset.reset", "latest");

    client = new KafkaConsumer<>(consumerConfig);
    log.trace(".init.ok");
  }

  @Override
  public byte[] readBytes() {
    throw new UnsupportedOperationException("not implemented");
  }

  public void assign() {
    TopicPartition partition = new TopicPartition(topic, partitionNumber);
    client.assign(Collections.singletonList(partition));
  }

  public ConsumerRecords<byte[], byte[]> poll(Duration duration) {
    return client.poll(duration);
  }
}
