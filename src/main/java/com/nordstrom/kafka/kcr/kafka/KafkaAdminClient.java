package com.nordstrom.kafka.kcr.kafka;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaAdminClient {
  private static final Logger log = LoggerFactory.getLogger(KafkaAdminClient.class);
  private final AdminClient client;

  public KafkaAdminClient(Properties config) {
    Properties adminConfig = new Properties();
    adminConfig.putAll(config);
    adminConfig.put("client.id", "kcr-admin-cid");

    client = AdminClient.create(adminConfig);
    log.trace("init.ok");
  }

  public int numberPartitions(String topic) {
    try {
      DescribeTopicsResult results = client.describeTopics(Collections.singletonList(topic));
      Map<String, TopicDescription> topicDescriptions = results.all().get();
      TopicDescription description = topicDescriptions.get(topic);
      if (description == null) {
        throw new RuntimeException("Topic not found: " + topic);
      }
      return description.partitions().size();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }
}
