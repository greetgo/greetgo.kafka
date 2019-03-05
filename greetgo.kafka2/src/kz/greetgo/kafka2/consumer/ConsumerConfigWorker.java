package kz.greetgo.kafka2.consumer;

import kz.greetgo.kafka2.core.config.ConfigEventRegistration;
import kz.greetgo.kafka2.core.config.ConfigEventType;
import kz.greetgo.kafka2.core.config.ConfigStorage;
import kz.greetgo.kafka2.util.ConfigLines;
import kz.greetgo.kafka2.util.Handler;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;

public class ConsumerConfigWorker implements AutoCloseable {
  private Supplier<ConfigStorage> configStorage;
  private Handler configDataChanged;
  private final AtomicReference<ConfigEventRegistration> configEventRegistration = new AtomicReference<>(null);

  private final AtomicReference<String> configPath = new AtomicReference<>(null);
  private final AtomicReference<String> parentPath = new AtomicReference<>(null);

  public void setParentPath(String parentPath) {
    this.parentPath.set(parentPath);
  }

  public void setConfigPath(String configPath) {
    this.configPath.set(configPath);
  }

  public ConsumerConfigWorker(Supplier<ConfigStorage> configStorage, Handler configDataChanged) {
    this.configStorage = configStorage;
    this.configDataChanged = configDataChanged;

    configEventRegistration.set(configStorage.get().addEventHandler((path, newContent, type) -> {
      if (Objects.equals(configPath.get(), path)) {
        performConfigEvent(newContent, type);
      }
    }));
  }

  private void performConfigEvent(byte[] newContent, ConfigEventType type) {
    throw new RuntimeException("Надо сделать");
  }

  @Override
  public void close() {
    ConfigEventRegistration reg = configEventRegistration.getAndSet(null);
    if (reg != null) {
      reg.unregister();
    }
  }

  public void start() {

    if (configPath.get() == null) {
      throw new IllegalStateException("configPath == null");
    }

    String configPath = this.configPath.get();

    ConfigStorage configStorage = this.configStorage.get();

    if (configStorage.exists(configPath)) {
      ConfigLines lines = ConfigLines.fromBytes(configStorage.readContent(configPath), configPath);
      if (lines != null) {
        String extendsValue = lines.getValue("extends");



      }
    }
  }

  private static List<String> contentToLines(byte[] content) {
    return new ArrayList<>(asList(new String(content, UTF_8).split("\n")));
  }

  /**
   * @return Количество необходимых воркеров. 0 - консюмер не работает. 1 - значение по-умолчанию
   */
  public int getWorkerCount() {
    throw new RuntimeException("Реализовать");
  }

  public Map<String, Object> getConfigMap() {
    throw new RuntimeException("Реализовать");
  }

  public Duration pollDuration() {
    return Duration.ofMillis(800);
  }
}
