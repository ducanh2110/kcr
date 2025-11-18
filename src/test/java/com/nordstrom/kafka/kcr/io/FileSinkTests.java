package com.nordstrom.kafka.kcr.io;

import static org.junit.jupiter.api.Assertions.*;

import com.nordstrom.kafka.kcr.facilities.AlphaNumKeyGenerator;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileSinkTests {
  private final AlphaNumKeyGenerator keyGen = new AlphaNumKeyGenerator();

  @Test
  void canCreateAFileSink(@TempDir Path tempDir) {
    String suffix = keyGen.key(8);
    String filepath = tempDir.resolve(suffix).toString();
    FileSink sink = new FileSink(null, filepath);
    assertNotNull(sink);
    assertTrue(sink.getPath().endsWith(suffix));
  }

  @Test
  void tryingToCreateFileSinkThatAlreadyExistsThrowsException(@TempDir Path tempDir)
      throws IOException {
    String filename = keyGen.key(8);
    File f = tempDir.resolve(filename).toFile();
    f.createNewFile();

    assertThrows(
        RuntimeException.class,
        () -> {
          new FileSink(null, f.getAbsolutePath());
        });
  }

  @Test
  void canCreateFileSinkWithParentFolder(@TempDir Path tempDir) {
    String filename = keyGen.key(8);
    String parentDir = tempDir.toString();
    FileSink sink = new FileSink(parentDir, filename);

    File p = new File(parentDir);
    assertTrue(p.isDirectory());
    assertTrue(sink.getPath().contains(filename));
    File f = new File(sink.getPath());
    assertTrue(f.isFile());
  }

  @Test
  void canCreateFileSinkWithParentFolderPath(@TempDir Path tempDir) {
    String filename = keyGen.key(8);
    String d1 = keyGen.key(8);
    String d2 = keyGen.key(8);
    String d3 = keyGen.key(8);
    String parentDir = tempDir + "/" + d1 + "/" + d2 + "/" + d3;
    FileSink sink = new FileSink(parentDir, filename);

    File p = new File(parentDir);
    assertTrue(p.isDirectory());
    assertTrue(p.getAbsolutePath().endsWith("/" + d1 + "/" + d2 + "/" + d3));

    File f = new File(sink.getPath());
    assertTrue(f.isFile());
    assertTrue(f.getAbsolutePath().endsWith("/" + d1 + "/" + d2 + "/" + d3 + "/" + filename));
  }
}
