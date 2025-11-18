package com.nordstrom.kafka.kcr.commands;

import com.nordstrom.kafka.kcr.Kcr;
import com.nordstrom.kafka.kcr.cassette.Cassette;
import com.nordstrom.kafka.kcr.cassette.CassetteInfo;
import com.nordstrom.kafka.kcr.io.FileSinkFactory;
import com.nordstrom.kafka.kcr.io.Sink;
import com.nordstrom.kafka.kcr.io.Source;
import com.nordstrom.kafka.kcr.kafka.KafkaAdminClient;
import com.nordstrom.kafka.kcr.kafka.KafkaSourceFactory;
import com.nordstrom.kafka.kcr.metrics.JmxConfigRecord;
import com.nordstrom.kafka.kcr.metrics.JmxNameMapper;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.jmx.JmxMeterRegistry;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;
import sun.misc.Signal;

@Command(name = "record", description = "Record a Kafka topic to a cassette.")
public class Record implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(Record.class);
  private static final String DEFAULT_CASSETTE_DIR = "kcr";
  private static final Pattern DURATION_PATTERN = Pattern.compile("(\\d.*)h(\\d.*)m(\\d.*)s");

  @ParentCommand private Kcr parent;

  @Option(
      names = {"--data-directory"},
      description =
          "Kafka Cassette Recorder data directory for recording (default=${DEFAULT-VALUE})",
      defaultValue = DEFAULT_CASSETTE_DIR)
  private String dataDirectory;

  @Option(
      names = {"--group-id"},
      description = "Kafka consumer group id (default=kcr-<topic>-gid)")
  private String groupId;

  @Option(
      names = {"--topic"},
      description = "Kafka topic to record (REQUIRED)",
      required = true)
  private String topic;

  @Option(
      names = {"--duration"},
      description = "Kafka duration for recording, format must be like **h**m**s")
  private String duration;

  @Option(
      names = {"--header-timestamp"},
      description = "Use timestamp from header parameter ignoring record timestamp",
      defaultValue = "")
  private String timestampHeaderName;

  @Option(
      names = {"--consumer-config"},
      description =
          "Optional Kafka Consumer configuration file. OVERWRITES any command-line values.")
  private String consumerConfig;

  private final CompositeMeterRegistry registry = new CompositeMeterRegistry();
  private final Instant start = new Date().toInstant();

  public Record() {
    registry.add(new JmxMeterRegistry(new JmxConfigRecord(), Clock.SYSTEM, new JmxNameMapper()));
  }

  @Override
  public void run() {
    boolean hasDuration = duration != null && !duration.isEmpty();

    if (hasDuration && !DURATION_PATTERN.matcher(duration).matches()) {
      System.err.println(
          "Duration must be in the format of **h**m**s, '**' must be integer or decimal. Please try again!");
      System.exit(1);
    }

    if (groupId != null && groupId.isEmpty()) {
      System.err.println("'group-id' value cannot be empty or null");
      System.exit(1);
    }

    Properties opts = parent.getOpts();
    System.out.println("kcr.record.id              : " + opts.get("kcr.id"));
    System.out.println("kcr.record.topic           : " + topic);

    Timer.Sample metricDurationTimer = Timer.start();

    // Remove non-kafka properties
    Properties cleanOpts = new Properties();
    cleanOpts.putAll(opts);
    cleanOpts.remove("kcr.id");

    // Add/overwrite consumer config from optional properties file.
    if (consumerConfig != null && !consumerConfig.isEmpty()) {
      try (FileInputStream insConsumerConfig = new FileInputStream(consumerConfig)) {
        Properties consumerOpts = new Properties();
        consumerOpts.load(insConsumerConfig);
        cleanOpts.putAll(consumerOpts);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    // Describe topic to get number of partitions to record.
    KafkaAdminClient admin = new KafkaAdminClient(cleanOpts);
    int numberPartitions = admin.numberPartitions(topic);
    System.out.println("kcr.record.topic.partitions: " + numberPartitions);
    System.out.println("kcr.record.duration        : " + duration);
    System.out.println("kcr.header.timestamp       : " + timestampHeaderName);

    // Create a cassette and start recording topic messages
    FileSinkFactory sinkFactory = new FileSinkFactory();
    KafkaSourceFactory sourceFactory =
        new KafkaSourceFactory(cleanOpts, topic, groupId, Kcr.getId());
    Cassette cassette =
        new Cassette(dataDirectory, topic, numberPartitions, sourceFactory, sinkFactory);
    cassette.create(String.valueOf(opts.get("kcr.id")));

    // Launch a Recorder thread for each partition using Virtual Threads
    ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    for (int partitionNumber = 0; partitionNumber < numberPartitions; partitionNumber++) {
      Source source = cassette.getSources().get(partitionNumber);
      Sink sink = cassette.getSinks().get(partitionNumber);
      Recorder recorder = new Recorder(source, sink, timestampHeaderName);
      int finalPartitionNumber = partitionNumber;

      executor.submit(
          () -> {
            Thread.currentThread().setName("kcr-recorder-" + finalPartitionNumber);
            recorder.record(finalPartitionNumber, registry);
          });
    }

    // Handle duration if specified
    if (hasDuration) {
      String[] parts = duration.split("h|m|s");
      long numDuration =
          (long)
              ((Double.parseDouble(parts[0]) * 3600000)
                  + (Double.parseDouble(parts[1]) * 60000)
                  + (Double.parseDouble(parts[2]) * 1000));
      try {
        Thread.sleep(numDuration);
        executor.shutdownNow();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        metricDurationTimer.stop(registry.timer("duration-ms"));
        CassetteInfo info = new CassetteInfo(cassette.getCassetteDir());
        System.out.println(info.summary());
        System.exit(0);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    // Handle ctrl-c
    log.trace(".run.wait-for-sigint");
    Signal.handle(
        new Signal("INT"),
        sig -> {
          metricDurationTimer.stop(registry.timer("duration-ms"));
          CassetteInfo info = new CassetteInfo(cassette.getCassetteDir());
          System.out.println(info.summary());
          System.exit(0);
        });

    // Update elapsed time metric whilst waiting for ctrl-c
    AtomicLong metricElapsedMillis = registry.gauge("elapsed-ms", new AtomicLong(0));
    while (true) {
      if (metricElapsedMillis != null) {
        metricElapsedMillis.set(Duration.between(start, new Date().toInstant()).toMillis());
      }
      try {
        Thread.sleep(500L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }
}
