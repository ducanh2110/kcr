package com.nordstrom.kafka.kcr.metrics;

import io.micrometer.statsd.StatsdConfig;
import io.micrometer.statsd.StatsdFlavor;

public class StatsdConfigKcr implements StatsdConfig {
  @Override
  public String get(String key) {
    return null;
  }

  @Override
  public StatsdFlavor flavor() {
    return StatsdFlavor.ETSY;
  }
}
