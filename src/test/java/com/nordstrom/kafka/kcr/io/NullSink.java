package com.nordstrom.kafka.kcr.io;

public class NullSink implements Sink {
  private String path = "";

  @Override
  public String getPath() {
    return path;
  }

  @Override
  public void setPath(String path) {
    this.path = path;
  }

  @Override
  public void writeText(String text) {
    // No-op
  }

  @Override
  public void writeBytes(byte[] bytes) {
    // No-op
  }
}
