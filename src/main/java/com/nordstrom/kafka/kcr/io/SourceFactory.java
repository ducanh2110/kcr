package com.nordstrom.kafka.kcr.io;

public interface SourceFactory {
  Source create(int partition);
}
