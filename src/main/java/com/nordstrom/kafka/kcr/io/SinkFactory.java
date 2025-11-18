package com.nordstrom.kafka.kcr.io;

public interface SinkFactory {
  Sink create(String parent, String name);
}
