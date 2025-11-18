package com.nordstrom.kafka.kcr.cassette;

import static org.junit.jupiter.api.Assertions.*;

import com.nordstrom.kafka.kcr.facilities.AlphaNumKeyGenerator;
import com.nordstrom.kafka.kcr.io.FileSinkFactory;
import com.nordstrom.kafka.kcr.io.NullSinkFactory;
import java.io.File;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CassetteTests {
  private final AlphaNumKeyGenerator keyGen = new AlphaNumKeyGenerator();

  @Test
  void nopTest() {
    assertTrue(true);
    assertFalse(false);
    assertDoesNotThrow(() -> {});
    assertThrows(
        Exception.class,
        () -> {
          throw new Exception("Il est mort, Jean Luc");
        });
  }

  @Test
  void cassetteTopicCannotBeNullOrBlank(@TempDir Path tempDir) {
    Exception e1 =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              new Cassette(null, null, 1, null, null);
            });
    assertTrue(e1.getMessage().contains("Topic cannot be null or blank"));

    Exception e2 =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              new Cassette(tempDir.toString(), "", 1, null, null);
            });
    assertTrue(e2.getMessage().contains("Topic cannot be null or blank"));
  }

  @Test
  void cassettePartitionsMustBeGreaterThan0(@TempDir Path tempDir) {
    Exception e1 =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              new Cassette(tempDir.toString(), keyGen.key(8), 0, null, null);
            });
    assertTrue(e1.getMessage().contains("Number of partitions must be > 0"));
  }

  @Test
  void canInstantiateCassette(@TempDir Path tempDir) {
    String topic = keyGen.key(8);
    Cassette cassette = new Cassette(tempDir.toString(), topic, 55, null, new NullSinkFactory());
    assertNotNull(cassette);
  }

  @Test
  void canCreateFileSinkCassette(@TempDir Path tempDir) {
    String topic = keyGen.key(8);
    Cassette cassette = new Cassette(tempDir.toString(), topic, 2, null, new FileSinkFactory());
    assertTrue(tempDir.toFile().isDirectory());
    cassette.create(keyGen.key(8));
    assertTrue(new File(cassette.getCassetteDir()).isDirectory());
    assertEquals(2, cassette.getSinks().size());
  }
}
