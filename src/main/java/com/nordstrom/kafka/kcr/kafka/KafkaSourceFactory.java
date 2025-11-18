package com.nordstrom.kafka.kcr.kafka;

import com.nordstrom.kafka.kcr.io.Source;
import com.nordstrom.kafka.kcr.io.SourceFactory;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSourceFactory implements SourceFactory {
  private static final Logger log = LoggerFactory.getLogger(KafkaSourceFactory.class);

  private final Properties config;
  private final String topic;
  private final String id;

  public KafkaSourceFactory(Properties sourceConfig, String topic, String groupId, String id) {
    this.topic = topic;
    this.id = id;

    config = new Properties();
    config.putAll(sourceConfig);

    String gid = groupId;
    if (groupId == null || groupId.isBlank()) {
      gid = "kcr-" + topic + "-gid-" + id;
    }
    config.put("group.id", gid);
  }

  @Override
  public Source create(int partition) {
    // Unique cid
    String cid = "kcr-" + topic + "-cid-" + id + "-" + partition;
    config.put("client.id", cid);

    return new KafkaSource(config, topic, partition);
  }
}
