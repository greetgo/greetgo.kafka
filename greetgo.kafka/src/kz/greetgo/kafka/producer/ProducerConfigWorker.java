package kz.greetgo.kafka.producer;

import kz.greetgo.kafka.core.config.ConfigEventType;
import kz.greetgo.kafka.core.config.EventConfigStorage;
import kz.greetgo.kafka.core.config.EventRegistration;
import kz.greetgo.kafka.util.ConfigLines;

import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class ProducerConfigWorker {
  private final Supplier<EventConfigStorage> configStorage;
  private final Consumer<ConfigLines> putDefaultValues;

  public ProducerConfigWorker(Supplier<EventConfigStorage> configStorage, Consumer<ConfigLines> putDefaultValues) {
    this.configStorage = configStorage;
    this.putDefaultValues = putDefaultValues;
  }

  private String configPath(String producerName) {
    return producerName + ".txt";
  }

  private final ConcurrentHashMap<String, ConfigLines> configLinesMap = new ConcurrentHashMap<>();

  public Map<String, Object> getConfigFor(String producerName) {
    String configPath = configPath(producerName);

    ConfigLines configLines = configLinesMap.computeIfAbsent(configPath, this::createConfigLinesAndSaveUpdateTimestamp);

    EventConfigStorage configStorage = this.configStorage.get();

    if (!configStorage.exists(configPath)) {
      configStorage.writeContent(configPath, configLines.toBytes());
    }

    configStorage.ensureLookingFor(configPath);
    ensureRegisteredHandler();

    return configLines.getWithPrefix("prod.");
  }

  private final AtomicReference<EventRegistration> registration = new AtomicReference<>(null);

  private void ensureRegisteredHandler() {

    if (this.registration.get() == null) {
      EventRegistration registration = configStorage.get().addEventHandler(this::configEventHappened);

      if (!this.registration.compareAndSet(null, registration)) {
        registration.unregister();
      }
    }

  }

  public void close() {
    EventRegistration registration = this.registration.getAndSet(null);
    if (registration != null) {
      registration.unregister();
    }
  }

  private void configEventHappened(String configPath, ConfigEventType type) {
    if (type != ConfigEventType.UPDATE) {
      return;
    }

    for (String key : new ArrayList<>(configLinesMap.keySet())) {
      if (Objects.equals(key, configPath)) {

        ConfigLines configLines = ConfigLines.fromBytes(configStorage.get().readContent(configPath), key);

        if (configLines == null) {
          configLinesMap.remove(key);
        } else {
          configLinesMap.put(key, configLines);
        }

        configPathUpdateTimestampMap.put(configPath, System.nanoTime());

      }
    }
  }

  public long getConfigUpdateTimestamp(String producerName) {
    String configPath = configPath(producerName);

    Long timestamp = configPathUpdateTimestampMap.get(configPath);

    return timestamp == null ? 0 : timestamp;
  }

  private final ConcurrentHashMap<String, Long> configPathUpdateTimestampMap = new ConcurrentHashMap<>();

  private ConfigLines createConfigLinesAndSaveUpdateTimestamp(String configPath) {
    ConfigLines ret = createConfigLines(configPath);
    configPathUpdateTimestampMap.put(configPath, System.nanoTime());
    return ret;
  }

  private ConfigLines createConfigLines(String configPath) {

    final byte[] configBytes;

    if (configStorage.get().exists(configPath)) {
      configBytes = configStorage.get().readContent(configPath);
    } else {
      configBytes = null;
    }

    if (configBytes != null) {
      return ConfigLines.fromBytes(configBytes, configPath);
    }

    {
      ConfigLines ret = new ConfigLines(configPath);

      putDefaultValues.accept(ret);

      return ret;
    }
  }
}
