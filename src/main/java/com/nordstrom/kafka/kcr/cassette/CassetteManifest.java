package com.nordstrom.kafka.kcr.cassette;

import com.nordstrom.kafka.kcr.io.Sink;
import com.nordstrom.kafka.kcr.io.SinkFactory;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassetteManifest {
  private static final Logger log = LoggerFactory.getLogger(CassetteManifest.class);
  private final Sink manifest;

  public CassetteManifest(
      SinkFactory sinkFactory,
      String directory,
      String id,
      String name,
      int partitions,
      String topic,
      Instant start) {
    manifest = sinkFactory.create(directory, topic + ".manifest");

    // TODO serialize as yaml
    manifest.writeText("---\n");
    manifest.writeText("directory:" + directory + "\n");
    manifest.writeText("id:" + id + "\n");
    manifest.writeText("name:" + name + "\n");
    manifest.writeText("partitions:" + partitions + "\n");
    manifest.writeText("topic:" + topic + "\n");
    manifest.writeText("version:" + CassetteVersion.VERSION + "\n");
    manifest.writeText("start:" + start);

    log.trace(".init.ok");
  }
}
