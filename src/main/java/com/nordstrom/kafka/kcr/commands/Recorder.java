package com.nordstrom.kafka.kcr.commands;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nordstrom.kafka.kcr.cassette.CassetteRecord;
import com.nordstrom.kafka.kcr.io.Sink;
import com.nordstrom.kafka.kcr.io.Source;
import com.nordstrom.kafka.kcr.kafka.KafkaSource;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.codec.binary.Hex;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Recorder {
  private static final Logger log = LoggerFactory.getLogger(Recorder.class);

  private final Source source;
  private final Sink sink;
  private final String timestampHeaderName;

  public Recorder(Source source, Sink sink, String timestampHeaderName) {
    this.source = source;
    this.sink = sink;
    this.timestampHeaderName = timestampHeaderName;
  }

  public void record(int partitionNumber, MeterRegistry registry) {
    Counter metricWriteTotal = registry.counter("write.total");
    Counter metricWrite =
        registry.counter("write.total", "partition", String.valueOf(partitionNumber));

    if (source instanceof KafkaSource kafkaSource) {
      kafkaSource.assign();
      ObjectMapper mapper = new ObjectMapper();

      while (true) {
        ConsumerRecords<byte[], byte[]> records = kafkaSource.poll(Duration.ofSeconds(20));
        for (ConsumerRecord<byte[], byte[]> it : records) {
          // The record does not always have a key so we need to test for that explicitly.
          String k = null;
          if (it.key() != null && it.key().length > 0) {
            k = new String(it.key(), Charset.defaultCharset());
          }

          long timestamp;
          if (it.timestampType() == TimestampType.NO_TIMESTAMP_TYPE || it.timestampType() == null) {
            timestamp = Instant.now().toEpochMilli();
          } else {
            timestamp = it.timestamp();
          }

          Map<String, String> headers = new HashMap<>();
          for (Header header : it.headers()) {
            headers.put(header.key(), new String(header.value()));
          }

          CassetteRecord record =
              new CassetteRecord(
                  headers,
                  timestamp,
                  it.partition(),
                  it.offset(),
                  k,
                  Hex.encodeHexString(it.value()));

          log.debug(
              ".record: ts={}, type={}, p={}, o={}, k={}",
              Instant.ofEpochMilli(it.timestamp()),
              it.timestampType(),
              it.partition(),
              it.offset(),
              k);

          if (timestampHeaderName != null && !timestampHeaderName.isBlank()) {
            record.withHeaderTimestamp(timestampHeaderName);
          }

          try {
            String data = mapper.writeValueAsString(record);
            log.debug(".record: {}", data);
            sink.writeText(data + "\n");
            metricWrite.increment();
            metricWriteTotal.increment();
          } catch (Exception e) {
            log.error("Error writing record", e);
          }
        }
      }
    }
  }
}
