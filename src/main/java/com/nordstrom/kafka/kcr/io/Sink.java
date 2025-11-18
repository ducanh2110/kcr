package com.nordstrom.kafka.kcr.io;

public interface Sink {
  String getPath();

  void setPath(String path);

  void writeText(String text);

  void writeBytes(byte[] bytes);
}
