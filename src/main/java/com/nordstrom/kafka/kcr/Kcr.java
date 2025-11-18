package com.nordstrom.kafka.kcr;

import com.nordstrom.kafka.kcr.cassette.CassetteVersion;
import com.nordstrom.kafka.kcr.commands.Play;
import com.nordstrom.kafka.kcr.commands.Record;
import com.nordstrom.kafka.kcr.facilities.AlphaNumKeyGenerator;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "kcr",
    description = "Apache Kafka topic record/playback tool",
    subcommands = {Play.class, Record.class},
    mixinStandardHelpOptions = true,
    version = "v" + KcrVersion.VERSION + "/" + CassetteVersion.VERSION)
public class Kcr implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(Kcr.class);
  private static final String ID = new AlphaNumKeyGenerator().key(8);

  @Option(
      names = {"--bootstrap-servers"},
      description = "Kafka bootstrap server list",
      defaultValue = "localhost:9092")
  private String bootstrapServers;

  @Option(
      names = {"--security-protocol"},
      description = "Security protocol",
      defaultValue = "PLAINTEXT")
  private String securityProtocol;

  @Option(
      names = {"--sasl-mechanism"},
      description = "SASL mechanism")
  private String saslMechanism;

  @Option(
      names = {"--sasl-username"},
      description = "SASL username",
      defaultValue = "")
  private String saslUsername;

  @Option(
      names = {"--sasl-password"},
      description = "SASL password",
      defaultValue = "")
  private String saslPassword;

  private final Properties opts = new Properties();

  @Override
  public void run() {
    opts.put("kcr.id", ID);
    opts.put("bootstrap.servers", bootstrapServers);
    opts.put("security.protocol", securityProtocol);

    if (securityProtocol.startsWith("SASL")) {
      if (saslMechanism != null) {
        opts.put("sasl.mechanism", saslMechanism);
      }

      String jaasConfig =
          switch (saslMechanism) {
            case "PLAIN" -> "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                + saslUsername
                + "\" password=\""
                + saslPassword
                + "\";";
            case "SCRAM-SHA-256",
                "SCRAM-SHA-512" -> "org.apache.kafka.common.security.scram.ScramLoginModule required username=\""
                + saslUsername
                + "\" password=\""
                + saslPassword
                + "\";";
            default -> ""; // Not supported
          };
      opts.put("sasl.jaas.config", jaasConfig);
    }
  }

  public Properties getOpts() {
    return opts;
  }

  public static String getId() {
    return ID;
  }

  public static void main(String[] args) {
    int exitCode = new CommandLine(new Kcr()).execute(args);
    System.exit(exitCode);
  }
}

class KcrVersion {
  public static final String VERSION = "2.0";

  private KcrVersion() {
    // Utility class
  }
}
