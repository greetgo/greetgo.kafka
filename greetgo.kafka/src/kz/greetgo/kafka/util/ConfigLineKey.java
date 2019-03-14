package kz.greetgo.kafka.util;

import static kz.greetgo.kafka.util.StrUtil.firstIndexOf;
import static kz.greetgo.kafka.util.StrUtil.spaces;
import static kz.greetgo.kafka.util.StrUtil.startSpacesCount;

public class ConfigLineKey {

  public static ConfigLineKey parse(String line) {

    if (line == null) {
      return null;
    }

    if (line.trim().startsWith("##")) {
      return null;
    }

    int index = firstIndexOf(line, '=', ':');
    if (index < 0) {
      return null;
    }

    return new ConfigLineKey(line.substring(0, index));
  }

  private ConfigLineKey(String text) {

    String trimmedText = text.trim();

    if (trimmedText.startsWith("#")) {
      commented = true;
      String subText = trimmedText.substring(1);
      key = subText.trim();
      paddingLeft1 = startSpacesCount(text);
      paddingLeft2 = startSpacesCount(subText) + 1;
    } else {
      commented = false;
      key = trimmedText;
      paddingLeft1 = 0;
      paddingLeft2 = startSpacesCount(text);
    }

    width = text.length() - paddingLeft1 - paddingLeft2;

  }

  private ConfigLineKey() {}

  private String key;
  private int paddingLeft1;
  private int paddingLeft2;
  private int width;
  private boolean commented;

  public ConfigLineKey copy() {
    ConfigLineKey ret = new ConfigLineKey();
    ret.key = key;
    ret.paddingLeft1 = paddingLeft1;
    ret.paddingLeft2 = paddingLeft2;
    ret.width = width;
    ret.commented = commented;
    return ret;
  }

  public String key() {
    return key;
  }

  public int paddingLeft1() {
    return paddingLeft1;
  }

  public int paddingLeft2() {
    return paddingLeft2;
  }

  public int width() {
    return width;
  }

  public boolean isCommented() {
    return commented;
  }

  public void setKey(String key) {
    if (key == null) {
      throw new IllegalArgumentException("key == null");
    }
    this.key = key;
    width = Math.max(width, key.length());
  }

  public void setCommented(boolean commented) {
    this.commented = commented;
    if (paddingLeft2 == 0) {
      paddingLeft2 = 1;
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(paddingLeft1 + paddingLeft2 + width + 10);
    sb.append(spaces(paddingLeft1));
    if (commented) {
      sb.append('#');
      if (paddingLeft2 > 1) {
        sb.append(spaces(paddingLeft2 - 1));
      }
    } else {
      sb.append(spaces(paddingLeft2));
    }
    sb.append(key);
    int needLength = paddingLeft1 + paddingLeft2 + width;
    while (sb.length() < needLength) {
      sb.append(' ');
    }
    return sb.toString();
  }
}
