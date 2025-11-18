package com.nordstrom.kafka.kcr.io;

public class NullSinkFactory implements SinkFactory {
  @Override
  public Sink create(String parent, String name) {
    return new NullSink();
  }
}
