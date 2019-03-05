package kz.greetgo.kafka2.util;

public class StrUtil {
  public static int startSpaces(String str) {
    for (int i = 0; i < str.length(); i++) {
      if (!Character.isWhitespace(str.charAt(i))) {
        return i;
      }
    }
    return 0;
  }

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
}
