package kz.greetgo.kafka.util;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

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

  public static String findFirstContains(List<String> list, String strPart) {
    for (String str : list) {
      if (str.contains(strPart)) {
        return str;
      }
    }
    return null;
  }

  public static byte[] linesToBytes(List<String> lines) {
    if (lines == null) {
      return null;
    }
    return String.join("\n", lines).getBytes(UTF_8);
  }

  public static List<String> bytesToLines(byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    return Arrays.stream(new String(bytes, UTF_8).split("\n")).collect(Collectors.toList());
  }

  public static void addIfAbsent(List<String> list, String str) {
    if (str == null) {
      return;
    }
    for (String element : list) {
      if (str.equals(element)) {
        return;
      }
    }
    list.add(str);
  }

  public static String intToStrLen(int intValue, int strLen) {
    StringBuilder sb = new StringBuilder(strLen);
    sb.append(intValue);
    while (sb.length() < strLen) {
      sb.insert(0, '0');
    }
    return sb.toString();
  }
}
