package com.nordstrom.kafka.kcr.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSink implements Sink {
  private static final Logger log = LoggerFactory.getLogger(FileSink.class);
  private final File file;

  public FileSink(String parent, String name) {
    log.trace(".init");
    try {
      if (parent == null || parent.isBlank()) {
        file = new File(name);
        if (!file.createNewFile()) {
          throw new RuntimeException(new FileAlreadyExistsException(file.getAbsolutePath()));
        }
      } else {
        File parentDir = new File(parent);
        parentDir.mkdirs();
        file = new File(parentDir, name);
        if (!file.createNewFile()) {
          throw new RuntimeException(new FileAlreadyExistsException(file.getAbsolutePath()));
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    log.trace(".init.ok:file={}", file.getAbsolutePath());
  }

  @Override
  public String getPath() {
    return file.getAbsolutePath();
  }

  @Override
  public void setPath(String path) {
    // Not implemented
  }

  @Override
  public void writeText(String text) {
    try (FileOutputStream fos = new FileOutputStream(file, true)) {
      fos.write(text.getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeBytes(byte[] bytes) {
    try (FileOutputStream fos = new FileOutputStream(file, true)) {
      fos.write(bytes);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
