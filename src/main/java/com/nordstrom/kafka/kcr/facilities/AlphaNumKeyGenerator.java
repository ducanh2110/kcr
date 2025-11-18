package com.nordstrom.kafka.kcr.facilities;

import java.util.Random;

public class AlphaNumKeyGenerator {
  private static final String CHAR_POOL =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  private final Random random = new Random();

  public String key(int len) {
    StringBuilder result = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      int index = random.nextInt(CHAR_POOL.length());
      result.append(CHAR_POOL.charAt(index));
    }
    return result.toString();
  }
}
