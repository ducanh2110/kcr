package com.nordstrom.kafka.kcr.cassette;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashMap;
import java.util.Map;

public class CassetteRecord {
  @JsonProperty("headers")
  private Map<String, String> headers = new HashMap<>();

  @JsonProperty("timestamp")
  private long timestamp;

  @JsonProperty("partition")
  private int partition;

  @JsonProperty("offset")
  private long offset;

  @JsonProperty("key")
  private String key;

  @JsonProperty("value")
  private String value;

  // Default constructor for Jackson
  public CassetteRecord() {}

  public CassetteRecord(
      Map<String, String> headers,
      long timestamp,
      int partition,
      long offset,
      String key,
      String value) {
    this.headers = headers != null ? headers : new HashMap<>();
    this.timestamp = timestamp;
    this.partition = partition;
    this.offset = offset;
    this.key = key;
    this.value = value;
  }

  public void withHeaderTimestamp(String key) {
    if (headers.containsKey(key)) {
      String value = headers.get(key);
      if (value != null) {
        try {
          timestamp = Long.parseLong(value);
        } catch (NumberFormatException e) {
          // Keep original timestamp if parsing fails
        }
      }
    }
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  public void setHeaders(Map<String, String> headers) {
    this.headers = headers;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public int getPartition() {
    return partition;
  }

  public void setPartition(int partition) {
    this.partition = partition;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }
}
