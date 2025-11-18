package com.nordstrom.kafka.kcr.commands;

import java.util.Properties;

public class NullRecorder {
  private final Properties config;
  private final String cassetteName;

  public NullRecorder(Properties config, String cassetteName) {
    this.config = config;
    this.cassetteName = cassetteName;
    System.out.println("kcr-null-recorder.init.OK");
  }

  public void record() {
    System.out.println("kcr-null-recorder.record");
  }
}
