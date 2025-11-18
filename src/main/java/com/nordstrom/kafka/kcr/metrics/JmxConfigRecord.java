package com.nordstrom.kafka.kcr.metrics;

import io.micrometer.jmx.JmxConfig;
import java.time.Duration;

public class JmxConfigRecord implements JmxConfig {
  public static final String DOMAIN = "kcr.recorder";

  @Override
  public String get(String key) {
    return null;
  }

  @Override
  public Duration step() {
    return Duration.ofSeconds(10);
  }

  @Override
  public String prefix() {
    return "jmx";
  }

  @Override
  public String domain() {
    return DOMAIN;
  }
}
