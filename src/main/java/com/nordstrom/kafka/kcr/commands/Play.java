package com.nordstrom.kafka.kcr.commands;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nordstrom.kafka.kcr.Kcr;
import com.nordstrom.kafka.kcr.cassette.CassetteInfo;
import com.nordstrom.kafka.kcr.cassette.CassetteRecord;
import com.nordstrom.kafka.kcr.kafka.KafkaAdminClient;
import com.nordstrom.kafka.kcr.metrics.JmxConfigPlay;
import com.nordstrom.kafka.kcr.metrics.JmxNameMapper;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.jmx.JmxMeterRegistry;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;
import sun.misc.Signal;

@Command(name = "play", description = "Playback a cassette to a Kafka topic.")
public class Play implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(Play.class);
  private static final Pattern DURATION_PATTERN = Pattern.compile("(\\d.*)h(\\d.*)m(\\d.*)s");

  @ParentCommand private Kcr parent;

  @Option(
      names = {"--cassette"},
      description = "Kafka Cassette Recorder directory for playback (REQUIRED)",
      required = true)
  private String cassette;

  @Option(
      names = {"--playback-rate"},
      description =
          "Playback rate multiplier (0 = playback as fast as possible, 1.0 = play at capture rate, 2.0 = playback at twice capture rate)",
      defaultValue = "1.0")
  private float playbackRate;

  @Option(
      names = {"--topic"},
      description = "Kafka topic to write (REQUIRED)",
      required = true)
  private String topic;

  @Option(
      names = {"--producer-config"},
      description =
          "Optional Kafka Producer configuration file. OVERWRITES any command-line values.")
  private String producerConfig;

  @Option(
      names = {"--info"},
      description = "List information about a Cassette, then exit")
  private boolean info;

  @Option(
      names = {"--pause"},
      description = "Pause at end of playback (ctrl-c to exit)")
  private boolean pause;

  @Option(
      names = {"--number-of-runs"},
      description = "Number of times to run the playback")
  private String numberOfPlays;

  @Option(
      names = {"--duration"},
      description = "Kafka duration for playback, format must be like **h**m**s")
  private String duration;

  private final CompositeMeterRegistry registry = new CompositeMeterRegistry();
  private final Instant start = new Date().toInstant();
  private int numberPartitions = 0;
  private AtomicLong metricElapsedMillis;

  public Play() {
    registry.add(new JmxMeterRegistry(new JmxConfigPlay(), Clock.SYSTEM, new JmxNameMapper()));
    metricElapsedMillis = registry.gauge("elapsed-ms", new AtomicLong(0));
  }

  @Override
  public void run() {
    File cassetteDir = new File(cassette);
    if (!cassetteDir.exists() || !cassetteDir.isDirectory()) {
      System.err.println("--cassette " + cassette + " is empty or invalid");
      System.exit(1);
    }

    boolean hasNumOfRuns = numberOfPlays != null && !numberOfPlays.isEmpty();
    boolean hasDuration = duration != null && !duration.isEmpty();

    if (hasNumOfRuns && hasDuration) {
      System.err.println("Error: option --number-of-runs cannot be used with --duration");
      System.exit(0);
    }

    if (hasDuration && !DURATION_PATTERN.matcher(duration).matches()) {
      System.err.println(
          "Duration must be in the format of **h**m**s, '**' must be integer or decimal. Please try again!");
      System.exit(1);
    }

    Properties opts = parent.getOpts();
    System.out.println("kcr.play.id      : " + opts.get("kcr.id"));
    System.out.println("kcr.play.topic   : " + topic);
    System.out.println("kcr.play.playback-rate: " + playbackRate);

    Timer.Sample metricDurationTimer = Timer.start();

    // Remove non-kafka properties
    Properties cleanOpts = new Properties();
    cleanOpts.putAll(opts);
    cleanOpts.remove("kcr.id");

    // Describe topic to get number of partitions of playback topic.
    KafkaAdminClient admin = new KafkaAdminClient(cleanOpts);
    numberPartitions = admin.numberPartitions(topic);

    // Read cassette info
    CassetteInfo cinfo = new CassetteInfo(cassette);
    System.out.println(cinfo.summary());
    if (info) {
      return;
    }
    if (cinfo.getTotalRecords() <= 0) {
      System.out.println("No records to play");
      return;
    }

    // Handle ctrl-c
    Signal.handle(
        new Signal("INT"),
        sig -> {
          System.out.println(".exit.");
          System.exit(0);
        });

    // Setup producer
    Properties producerOpts = new Properties();
    producerOpts.put("key.serializer", ByteArraySerializer.class.getCanonicalName());
    producerOpts.put("value.serializer", ByteArraySerializer.class.getCanonicalName());
    producerOpts.put("client.id", "kcr-" + topic + "-cid-" + opts.get("kcr.id") + "}");

    if (producerConfig != null && !producerConfig.isEmpty()) {
      try (FileInputStream insProducerConfig = new FileInputStream(producerConfig)) {
        Properties prodConfigProps = new Properties();
        prodConfigProps.load(insProducerConfig);
        cleanOpts.putAll(prodConfigProps);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    producerOpts.putAll(cleanOpts);
    KafkaProducer<byte[], byte[]> client = new KafkaProducer<>(producerOpts);

    String[] filelist = cassetteDir.list();
    if (filelist == null) {
      System.err.println("Cannot read cassette directory");
      System.exit(1);
    }

    Instant startKcr = new Date().toInstant();

    if (hasDuration) {
      runWithDuration(cinfo, client, filelist);
    } else {
      runWithCount(cinfo, client, filelist, hasNumOfRuns);
    }

    System.out.println("kcr.play.runtime : " + Duration.between(startKcr, new Date().toInstant()));
    metricDurationTimer.stop(registry.timer("duration-ms"));

    if (pause) {
      Signal.handle(new Signal("INT"), sig -> System.exit(0));
      while (true) {
        try {
          Thread.sleep(500L);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  }

  private void runWithDuration(
      CassetteInfo cinfo, KafkaProducer<byte[], byte[]> client, String[] filelist) {
    String[] parts = duration.split("h|m|s");
    long timeLeftMillis =
        (long)
            ((Double.parseDouble(parts[0]) * 3600000)
                + (Double.parseDouble(parts[1]) * 60000)
                + (Double.parseDouble(parts[2]) * 1000));

    AtomicBoolean abort = new AtomicBoolean(false);
    long startTime = System.currentTimeMillis();

    while (!abort.get() && shouldContinueWithDuration(timeLeftMillis)) {
      long runStart = System.currentTimeMillis();

      try {
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        long offsetNanos = ChronoUnit.NANOS.between(cinfo.getEarliest(), new Date().toInstant());

        for (String fileName : filelist) {
          if (!fileName.contains("manifest")) {
            log.trace(".run:file={}", fileName);
            executor.submit(
                () -> {
                  try {
                    processFile(fileName, client, offsetNanos);
                  } catch (Exception e) {
                    log.error("Error processing file", e);
                  }
                });
          }
        }

        executor.shutdown();
        long timeout = Math.max(1, timeLeftMillis);
        if (!executor.awaitTermination(timeout, TimeUnit.MILLISECONDS)) {
          executor.shutdownNow();
          abort.set(true);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        abort.set(true);
      }

      long dt = System.currentTimeMillis() - runStart;
      timeLeftMillis -= dt;
    }
  }

  private void runWithCount(
      CassetteInfo cinfo,
      KafkaProducer<byte[], byte[]> client,
      String[] filelist,
      boolean hasNumOfRuns) {
    int iRuns = 0;
    int maxRuns = hasNumOfRuns && numberOfPlays != null ? Integer.parseInt(numberOfPlays) : 1;

    while (shouldContinueWithCount(iRuns, hasNumOfRuns, maxRuns)) {
      try {
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        long offsetNanos = ChronoUnit.NANOS.between(cinfo.getEarliest(), new Date().toInstant());

        for (String fileName : filelist) {
          if (!fileName.contains("manifest")) {
            log.trace(".run:file={}", fileName);
            executor.submit(
                () -> {
                  try {
                    processFile(fileName, client, offsetNanos);
                  } catch (Exception e) {
                    log.error("Error processing file", e);
                  }
                });
          }
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      iRuns++;
    }
  }

  private void processFile(
      String fileName, KafkaProducer<byte[], byte[]> client, long offsetNanos) {
    String partitionNumber = fileName.substring(fileName.lastIndexOf("-") + 1);
    Timer.Sample metricDurationTimer = Timer.start();
    Counter metricSend = registry.counter("send.total", "partition", partitionNumber);
    Counter metricSendTotal = registry.counter("send.total");
    ObjectMapper mapper = new ObjectMapper();

    try (BufferedReader reader = new BufferedReader(new FileReader(new File(cassette, fileName)))) {
      String line;
      while ((line = reader.readLine()) != null) {
        CassetteRecord record = mapper.readValue(line, CassetteRecord.class);
        play(client, record, offsetNanos);
        metricSend.increment();
        metricSendTotal.increment();
        updateElapsed();
      }
    } catch (IOException e) {
      log.error("Error reading cassette file", e);
    }

    metricDurationTimer.stop(registry.timer("duration-ms", "partition", partitionNumber));
  }

  private void play(KafkaProducer<byte[], byte[]> client, CassetteRecord record, long offsetNanos) {
    Instant ts = new Date(record.getTimestamp()).toInstant();
    Instant now = new Date().toInstant();
    Instant whenToSend = ts.plusNanos(offsetNanos);
    Duration wait = Duration.between(now, whenToSend);

    long millis = playbackRate > 0.0 ? (long) (wait.toMillis() / playbackRate) : 0L;

    if (millis > 0) {
      try {
        Thread.sleep(millis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    int partitionToUse = mapPartition(record.getPartition(), numberPartitions);
    byte[] key = record.getKey() != null ? record.getKey().getBytes() : null;
    byte[] value;
    try {
      value = Hex.decodeHex(record.getValue());
    } catch (DecoderException e) {
      log.error("Error decoding hex value", e);
      return;
    }

    ProducerRecord<byte[], byte[]> producerRecord =
        new ProducerRecord<>(topic, partitionToUse, key, value);

    for (Map.Entry<String, String> header : record.getHeaders().entrySet()) {
      producerRecord.headers().add(header.getKey(), header.getValue().getBytes());
    }

    try {
      client.send(producerRecord).get();
    } catch (Exception e) {
      System.err.println(
          "ERROR during send: partition="
              + record.getPartition()
              + ", key="
              + record.getKey()
              + ", exception="
              + e);
    }
  }

  private void updateElapsed() {
    if (metricElapsedMillis != null) {
      metricElapsedMillis.set(Duration.between(start, new Date().toInstant()).toMillis());
    }
  }

  private boolean shouldContinueWithDuration(long timeLeftMillis) {
    return timeLeftMillis > 0;
  }

  private boolean shouldContinueWithCount(int runCount, boolean hasNumOfRuns, int maxRuns) {
    if (!hasNumOfRuns && runCount == 0) {
      return true;
    } else if (hasNumOfRuns && maxRuns == 0) {
      return true;
    } else if (hasNumOfRuns && runCount < maxRuns) {
      return true;
    }
    return false;
  }

  private int mapPartition(int partition, int numberPartitions) {
    return partition % numberPartitions;
  }
}
