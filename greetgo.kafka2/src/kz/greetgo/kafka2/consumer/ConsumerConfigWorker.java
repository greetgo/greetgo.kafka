package kz.greetgo.kafka2.consumer;

import kz.greetgo.kafka2.core.config.ConfigEventRegistration;
import kz.greetgo.kafka2.core.config.ConfigEventType;
import kz.greetgo.kafka2.core.config.ConfigStorage;
import kz.greetgo.kafka2.util.ConfigLineCommand;
import kz.greetgo.kafka2.util.ConfigLines;
import kz.greetgo.kafka2.util.Handler;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

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

  ConfigLines configLines;

  public void start() {

    prepareConfigLines();

    setDefaultValues();

    {
      ConfigStorage configStorage = this.configStorage.get();
      configStorage.writeContent(configLines.getConfigPath(), configLines.toBytes());

      ConfigLines parent = configLines.parent;
      if (parent != null) {
        configStorage.writeContent(parent.getConfigPath(), parent.toBytes());
      }
    }
  }

  private void prepareConfigLines() {
    String configPath = this.configPath.get();

    if (configPath == null) {
      throw new IllegalStateException("configPath == null");
    }

    ConfigStorage configStorage = this.configStorage.get();
    ConfigLines itConfigLines = ConfigLines.fromBytes(configStorage.readContent(configPath), configPath);
    if (itConfigLines == null) {
      itConfigLines = new ConfigLines(configPath);
      String parentPath = this.parentPath.get();
      if (parentPath != null) {
        itConfigLines.putValue("extends", parentPath);
      }
    }

    String parentPath = itConfigLines.getValue("extends");
    if (parentPath != null) {
      itConfigLines.parent = ConfigLines.fromBytes(configStorage.readContent(parentPath), parentPath);
      if (itConfigLines.parent == null) {
        itConfigLines.parent = new ConfigLines(parentPath);
      }
    }

    configLines = itConfigLines;
  }

  private void setDefaultValues() {
    ConsumerConfigDefaults defaults = new ConsumerConfigDefaults();

    for (Map.Entry<String, String> e : defaults.patentableValues.entrySet()) {

      ConfigLines parent = configLines.parent;

      if (configLines.existsValueOrCommand(e.getKey())) {

        if (parent != null) {
          if (!parent.existsValueOrCommand(e.getKey())) {
            parent.putValue(e.getKey(), e.getValue());
          }
        }

        continue;
      }

      if (parent != null) {
        if (!parent.existsValueOrCommand(e.getKey())) {
          parent.putValue(e.getKey(), e.getValue());
        }

        configLines.putCommand(e.getKey(), ConfigLineCommand.INHERITS);
      } else {
        configLines.putValue(e.getKey(), e.getValue());
      }

    }

    for (Map.Entry<String, String> e : defaults.ownValues.entrySet()) {

      if (configLines.existsValueOrCommand(e.getKey())) {
        continue;
      }

      configLines.putValue(e.getKey(), e.getValue());

    }
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
