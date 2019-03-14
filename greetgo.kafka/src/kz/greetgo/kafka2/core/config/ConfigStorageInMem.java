package kz.greetgo.kafka2.core.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static kz.greetgo.kafka2.util.StrUtil.intToStrLen;

public class ConfigStorageInMem extends ConfigStorageAbstract {

  private final ConcurrentHashMap<String, byte[]> data = new ConcurrentHashMap<>();

  @Override
  public boolean exists(String path) {
    return data.containsKey(path);
  }

  @Override
  public byte[] readContent(String path) {
    return data.get(path);
  }

  @Override
  public void writeContent(String path, byte[] content) {
    if (content == null) {
      data.remove(path);
    } else {
      data.put(path, content);
    }
  }

  private final Map<String, byte[]> state = new HashMap<>();

  public void rememberState() {
    state.clear();
    for (Map.Entry<String, byte[]> e : data.entrySet()) {
      state.put(e.getKey(), e.getValue());
    }
  }

  public void fireEvents() {

    for (String path : lookingForPaths.keySet()) {
      byte[] nowBytes = data.get(path);
      byte[] oldBytes = state.get(path);
      if (nowBytes != null && oldBytes != null) {
        if (!Arrays.equals(nowBytes, oldBytes)) {
          fireConfigEventHandler(path, ConfigEventType.UPDATE);
        }
        continue;
      }
      //noinspection ConstantConditions
      if (nowBytes != null && oldBytes == null) {
        fireConfigEventHandler(path, ConfigEventType.CREATE);
        continue;
      }
      //noinspection ConstantConditions
      if (nowBytes == null && oldBytes != null) {
        fireConfigEventHandler(path, ConfigEventType.DELETE);
        continue;
      }
    }

    rememberState();
  }

  private final ConcurrentHashMap<String, Boolean> lookingForPaths = new ConcurrentHashMap<>();

  @Override
  public void ensureLookingFor(String path) {
    lookingForPaths.put(path, true);
  }

  public List<String> getLinesWithoutSpaces(String path) {
    byte[] bytes = data.get(path);
    if (bytes == null) {
      return null;
    }

    return new ArrayList<>(Arrays.asList(new String(bytes, UTF_8)
      .split("\\n")))
      .stream()
      .map(s -> s.replaceAll("\\s+", ""))
      .collect(toList());
  }

  private static byte[] addLines(byte[] bytes, String[] lines) {
    List<String> lineList = bytes == null
      ? new ArrayList<>() :
      new ArrayList<>(Arrays.asList(new String(bytes, UTF_8).split("\\n")));

    lineList.addAll(Arrays.asList(lines));

    return String.join("\n", lineList).getBytes(UTF_8);
  }

  private static byte[] removeLines(byte[] bytes, String[] lines) {
    if (bytes == null) {
      return null;
    }
    List<String> lineList = new ArrayList<>(Arrays.asList(new String(bytes, UTF_8).split("\\n")));
    lineList.removeAll(Arrays.asList(lines));
    return lineList.isEmpty() ? null : String.join("\n", lineList).getBytes(UTF_8);
  }

  @SuppressWarnings("Duplicates")
  public void addLines(String path, String... lines) {
    while (true) {
      byte[] bytes = data.get(path);

      byte[] newBytes = addLines(bytes, lines);

      if (bytes == null) {
        if (data.putIfAbsent(path, newBytes) == null) {
          return;
        }
        continue;
      }
      if (data.replace(path, bytes, newBytes)) {
        return;
      }
    }
  }

  @SuppressWarnings("Duplicates")
  public void removeLines(String path, String... lines) {
    while (true) {
      byte[] bytes = data.get(path);

      byte[] newBytes = removeLines(bytes, lines);

      if (Arrays.equals(bytes, newBytes)) {
        return;
      }

      if (newBytes == null) {
        if (data.remove(path, bytes)) {
          return;
        }
        continue;
      }

      if (data.replace(path, bytes, newBytes)) {
        return;
      }
    }
  }

  public void printCurrentState() {
    List<Map.Entry<String, byte[]>> list
      = data
      .entrySet()
      .stream()
      .sorted(Comparator.comparing(Map.Entry::getKey))
      .collect(toList());

    for (Map.Entry<String, byte[]> e : list) {

      System.out.println("FILE " + e.getKey());

      String[] lines = new String(e.getValue(), UTF_8).split("\n");

      if (lines.length > 0) {
        int len = ("" + (lines.length - 1)).length();
        int no = 1;
        for (String line : lines) {
          System.out.println("LINE " + intToStrLen(no++, len) + " : " + line);
        }
      }

      System.out.println();

    }
  }
}
