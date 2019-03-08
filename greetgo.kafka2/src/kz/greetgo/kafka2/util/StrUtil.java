package kz.greetgo.kafka2.util;

import java.util.Arrays;

public class StrUtil {

  public static String expandStr(String value, int valueSpace) {
    if (value == null) {
      return null;
    }
    if (value.length() >= valueSpace) {
      return value;
    }
    char[] ret = new char[valueSpace];
    value.getChars(0, value.length(), ret, 0);
    for (int i = value.length(); i < valueSpace; i++) {
      ret[i] = ' ';
    }
    return new String(ret);
  }

  public static String spaces(int spaceCount) {
    char[] ret = new char[spaceCount];
    for (int i = 0; i < spaceCount; i++) {
      ret[i] = ' ';
    }
    return new String(ret);
  }

  public static int firstIndexOf(String str, char... chars) {
    if (str == null) {
      return -1;
    }

    int[] indexes = new int[chars.length];
    for (int i = 0; i < chars.length; i++) {
      indexes[i] = str.indexOf(chars[i]);
    }

    return Arrays.stream(indexes).filter(x -> x >= 0).min().orElse(-1);
  }

  public static int startSpacesCount(String str) {
    if (str == null) {
      return 0;
    }

    for (int i = 0; i < str.length(); i++) {
      if (!Character.isWhitespace(str.charAt(i))) {
        return i;
      }
    }

    return str.length();
  }
}
