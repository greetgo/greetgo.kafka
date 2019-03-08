package kz.greetgo.kafka2.core.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;

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

  @Override
  public void ensureLookingFor(String path) {}

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
    List<String> lineList = bytes == null ? new ArrayList<>() : Arrays.asList(new String(bytes, UTF_8).split("\\n"));
    lineList.addAll(Arrays.asList(lines));
    return String.join("\n", lineList).getBytes(UTF_8);
  }

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
}
