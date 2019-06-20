package kz.greetgo.kafka.util;

import kz.greetgo.kafka.core.config.ConfigEventType;
import kz.greetgo.kafka.core.config.EventConfigFile;
import kz.greetgo.kafka.core.config.EventFileHandler;
import kz.greetgo.kafka.core.config.EventRegistration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;

public class TestEventConfigFile implements EventConfigFile {

  private final ConcurrentHashMap<Long, EventFileHandler> map = new ConcurrentHashMap<>();

  private final AtomicLong nextId = new AtomicLong(1);

  @Override
  public EventRegistration addEventHandler(EventFileHandler eventFileHandler) {
    final long id = nextId.getAndIncrement();
    map.put(id, eventFileHandler);
    return () -> map.remove(id);
  }

  public void fireEvent(ConfigEventType type) {
    map.values().forEach(x -> x.eventHappened(type));
  }

  boolean ensureLookingWorking = false;

  @Override
  public void ensureLookingFor() {
    if (exists()) {
      ensureLookingWorking = true;
    }
  }

  private Date createdAt = null;

  @Override
  public Optional<Date> createdAt() {
    return Optional.ofNullable(createdAt);
  }

  private Date lastModifiedAt = null;

  @Override
  public Optional<Date> lastModifiedAt() {
    return Optional.ofNullable(lastModifiedAt);
  }

  private List<String> content = null;

  @Override
  public byte[] readContent() {
    if (content == null) {
      return null;
    }
    return String.join("\n", content).getBytes(UTF_8);
  }

  public int writeContentCount = 0;

  @Override
  public void writeContent(byte[] content) {
    writeContentCount++;
    if (content == null) {
      this.content = null;
      createdAt = null;
      lastModifiedAt = null;
      return;
    }
    Date now = new Date();
    if (this.content == null) {
      createdAt = now;
    }
    this.content = bytesToList(content);
    lastModifiedAt = now;
  }

  private static List<String> bytesToList(byte[] content) {
    if (content == null) {
      return null;
    }

    return Arrays.stream(new String(content, UTF_8).split("\n")).collect(toList());
  }

  public List<String> readLines() {
    List<String> content = this.content;

    if (content == null) {
      return null;
    }

    return new ArrayList<>(content);
  }

  public List<String> readLinesWithoutSpaces() {
    List<String> content = this.content;

    if (content == null) {
      return null;
    }

    return content
      .stream()
      .map(s -> s.replaceAll("\\s+", ""))
      .collect(toList());

  }

  @Override
  public void close() {
    map.clear();
  }

  public void addLines(String... lines) {
    List<String> content = new ArrayList<>();
    boolean set = true;

    {
      List<String> c = this.content;
      if (c != null) {
        content = c;
        set = false;
      }
    }

    content.addAll(Arrays.asList(lines));

    if (set) {
      this.content = content;
    }
  }

  public void delLines(String... lines) {
    List<String> content = this.content;
    if (content == null) {
      return;
    }

    for (String line : lines) {
      content.remove(line);
    }

  }
}
