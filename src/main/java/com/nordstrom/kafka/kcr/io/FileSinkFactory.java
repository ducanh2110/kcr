package com.nordstrom.kafka.kcr.io;

public class FileSinkFactory implements SinkFactory {
  @Override
  public Sink create(String parent, String name) {
    return new FileSink(parent, name);
  }
}
