package com.nordstrom.kafka.kcr.cassette;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class CassetteInfo {
  private final String cassette;
  private final Instant earliest;
  private final Instant latest;
  private final List<CassettePartitionInfo> partitions = new ArrayList<>();
  private final int totalRecords;
  private final Duration cassetteLength;

  public CassetteInfo(String cassette) {
    this.cassette = cassette;

    File cassetteDir = new File(cassette);
    String[] filelist = cassetteDir.list();

    if (filelist != null) {
      for (String file : filelist) {
        if (!file.contains("manifest")) {
          CassettePartitionInfo partition = new CassettePartitionInfo(cassette, file);
          partitions.add(partition);
        }
      }
    }

    long t0 =
        partitions.stream().map(CassettePartitionInfo::getEarliest).min(Long::compareTo).orElse(0L);
    long t1 =
        partitions.stream().map(CassettePartitionInfo::getLatest).max(Long::compareTo).orElse(0L);

    earliest = new Date(t0).toInstant();
    latest = new Date(t1).toInstant();
    cassetteLength = Duration.between(earliest, latest);
    totalRecords = partitions.stream().mapToInt(CassettePartitionInfo::getCount).sum();
  }

  public String summary() {
    return " _________\n"
        + "|   ___   | title   : "
        + cassette
        + "\n"
        + "|  o___o  | tracks  : "
        + partitions.size()
        + "\n"
        + "|__/___\\__| songs   : "
        + totalRecords
        + "\n"
        + "            recorded: "
        + earliest
        + " - "
        + latest
        + "\n"
        + "            length  : "
        + cassetteLength;
  }

  public Instant getEarliest() {
    return earliest;
  }

  public Instant getLatest() {
    return latest;
  }

  public int getTotalRecords() {
    return totalRecords;
  }

  public Duration getCassetteLength() {
    return cassetteLength;
  }

  public static class CassettePartitionInfo {
    private final long earliest;
    private final long latest;
    private final int count;

    public CassettePartitionInfo(String cassette, String file) {
      ObjectMapper mapper = new ObjectMapper();
      long first = Long.MAX_VALUE;
      long last = Long.MIN_VALUE;
      int recordCount = 0;

      try (BufferedReader reader = new BufferedReader(new FileReader(new File(cassette, file)))) {
        String line;
        while ((line = reader.readLine()) != null) {
          CassetteRecord record = mapper.readValue(line, CassetteRecord.class);
          if (record.getTimestamp() < first) {
            first = record.getTimestamp();
          }
          if (record.getTimestamp() > last) {
            last = record.getTimestamp();
          }
          recordCount++;
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      earliest = first;
      latest = last;
      count = recordCount;
    }

    public long getEarliest() {
      return earliest;
    }

    public long getLatest() {
      return latest;
    }

    public int getCount() {
      return count;
    }
  }
}
