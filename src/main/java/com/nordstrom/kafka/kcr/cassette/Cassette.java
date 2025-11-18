package com.nordstrom.kafka.kcr.cassette;

import com.nordstrom.kafka.kcr.io.Sink;
import com.nordstrom.kafka.kcr.io.SinkFactory;
import com.nordstrom.kafka.kcr.io.Source;
import com.nordstrom.kafka.kcr.io.SourceFactory;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cassette {
  private static final Logger log = LoggerFactory.getLogger(Cassette.class);

  private final String dataDirectory;
  private final String topic;
  private final int partitions;
  private final SourceFactory sourceFactory;
  private final SinkFactory sinkFactory;

  private String cassetteDir;
  private String cassetteName;
  private CassetteManifest manifest;
  private final List<Sink> sinks = new ArrayList<>();
  private final List<Source> sources = new ArrayList<>();

  public Cassette(
      String dataDirectory,
      String topic,
      int partitions,
      SourceFactory sourceFactory,
      SinkFactory sinkFactory) {
    if (topic == null || topic.isBlank()) {
      throw new IllegalArgumentException("Topic cannot be null or blank");
    }
    if (partitions <= 0) {
      throw new IllegalArgumentException("Number of partitions must be > 0");
    }
    if (sinkFactory == null) {
      throw new IllegalArgumentException("Must have a concrete SinkFactory");
    }

    this.dataDirectory = dataDirectory;
    this.topic = topic;
    this.partitions = partitions;
    this.sourceFactory = sourceFactory;
    this.sinkFactory = sinkFactory;
  }

  public void create(String id) {
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HHmmss", Locale.getDefault());
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    Date nowish = new Date();
    cassetteName = "kcr-" + topic + "-" + dateFormat.format(nowish);
    cassetteDir = dataDirectory + "/" + cassetteName;

    // Create manifest
    manifest =
        new CassetteManifest(
            sinkFactory, cassetteDir, id, cassetteName, partitions, topic, new Date().toInstant());

    // Create a sink for each partition
    for (int partition = 0; partition < partitions; partition++) {
      String partitionName = topic + "-" + partition;
      Sink sink = sinkFactory.create(cassetteDir, partitionName);
      sinks.add(sink);
    }

    // Create a source for each partition
    if (sourceFactory != null) {
      for (int partition = 0; partition < partitions; partition++) {
        Source source = sourceFactory.create(partition);
        sources.add(source);
      }
    }
  }

  public String getCassetteDir() {
    return cassetteDir;
  }

  public String getCassetteName() {
    return cassetteName;
  }

  public List<Sink> getSinks() {
    return sinks;
  }

  public List<Source> getSources() {
    return sources;
  }
}
