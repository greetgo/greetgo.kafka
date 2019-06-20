package kz.greetgo.kafka.core.config;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static kz.greetgo.kafka.util.StrUtil.bytesToLines;
import static kz.greetgo.kafka.util.StrUtil.intToStrLen;

public class EventConfigStorageInMem extends EventConfigStorageAbstract {

  private static class FileRecord {
    final Date createdAt;
    final Date lastModifiedAt;
    final byte[] data;

    public FileRecord(Date createdAt, byte[] data) {
      this.createdAt = createdAt;
      this.lastModifiedAt = new Date();
      this.data = data;
    }

    public FileRecord(byte[] data) {
      createdAt = new Date();
      this.lastModifiedAt = createdAt;
      this.data = data;
    }

    public static FileRecord create(byte[] data) {
      Objects.requireNonNull(data);
      return new FileRecord(data);
    }

    public FileRecord modify(byte[] data) {
      Objects.requireNonNull(data);
      return new FileRecord(createdAt, data);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      FileRecord that = (FileRecord) o;
      return Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(data);
    }
  }

  private final ConcurrentHashMap<String, FileRecord> data = new ConcurrentHashMap<>();

  @Override
  public byte[] readContent(String path) {
    return Optional.ofNullable(data.get(path)).map(a -> a.data).orElse(null);
  }

  public List<String> readLines(String path) {
    return bytesToLines(readContent(path));
  }

  @Override
  public void writeContent(String path, byte[] content) {
    if (content == null) {
      data.remove(path);
    } else {
      data.compute(path, (path1, fileRecord)
        -> fileRecord == null ? FileRecord.create(content) : fileRecord.modify(content));
    }
  }

  @Override
  public Optional<Date> createdAt(String path) {
    return Optional.ofNullable(data.get(path)).map(a -> a.createdAt);
  }

  @Override
  public Optional<Date> lastModifiedAt(String path) {
    return Optional.ofNullable(data.get(path)).map(a -> a.lastModifiedAt);
  }

  private final Map<String, FileRecord> state = new HashMap<>();

  public void rememberState() {
    state.clear();
    for (Map.Entry<String, FileRecord> e : data.entrySet()) {
      state.put(e.getKey(), e.getValue());
    }
  }

  public void fireEvents() {

    for (String path : lookingForPaths.keySet()) {
      FileRecord nowBytes = data.get(path);
      FileRecord oldBytes = state.get(path);
      if (nowBytes != null && oldBytes != null) {
        if (!Objects.equals(nowBytes, oldBytes)) {
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

  }

  private final ConcurrentHashMap<String, Boolean> lookingForPaths = new ConcurrentHashMap<>();

  @Override
  public void ensureLookingFor(String path) {
    lookingForPaths.put(path, true);
  }

  public List<String> getLinesWithoutSpaces(String path) {

    FileRecord bytes = data.get(path);
    if (bytes == null) {
      return null;
    }

    return new ArrayList<>(Arrays.asList(new String(bytes.data, UTF_8)
      .split("\\n")))
      .stream()
      .map(s -> s.replaceAll("\\s+", ""))
      .collect(toList());

  }

  private static FileRecord addLines(FileRecord fileRecord, String[] lines) {

    List<String> lineList = fileRecord == null
      ? new ArrayList<>() :
      new ArrayList<>(Arrays.asList(new String(fileRecord.data, UTF_8).split("\\n")));

    lineList.addAll(Arrays.asList(lines));

    byte[] newBytes = String.join("\n", lineList).getBytes(UTF_8);

    return fileRecord == null ? FileRecord.create(newBytes) : fileRecord.modify(newBytes);

  }

  private static FileRecord removeLines(FileRecord fileRecord, String[] lines) {

    if (fileRecord == null) {
      return null;
    }
    List<String> lineList = new ArrayList<>(Arrays.asList(new String(fileRecord.data, UTF_8).split("\\n")));

    lineList.removeAll(Arrays.asList(lines));

    if (lineList.isEmpty()) {
      return null;
    }

    return fileRecord.modify(String.join("\n", lineList).getBytes(UTF_8));

  }

  @SuppressWarnings("Duplicates")
  public void addLines(String path, String... lines) {

    while (true) {
      FileRecord bytes = data.get(path);

      FileRecord newBytes = addLines(bytes, lines);

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
      FileRecord bytes = data.get(path);

      FileRecord newBytes = removeLines(bytes, lines);

      if (Objects.equals(bytes, newBytes)) {
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

  private void writeLine(String line) {
    PrintStream out = System.out;
    out.println(line);
  }

  public void printCurrentState() {
    List<Map.Entry<String, FileRecord>> list
      = data
      .entrySet()
      .stream()
      .sorted(Comparator.comparing(Map.Entry::getKey))
      .collect(toList());

    for (Map.Entry<String, FileRecord> e : list) {

      writeLine("FILE " + e.getKey());

      String[] lines = new String(e.getValue().data, UTF_8).split("\n");

      if (lines.length > 0) {
        int len = ("" + (lines.length - 1)).length();
        int no = 1;
        for (String line : lines) {
          writeLine("LINE " + intToStrLen(no++, len) + " : " + line);
        }
      }

      writeLine("");

    }
  }
}
