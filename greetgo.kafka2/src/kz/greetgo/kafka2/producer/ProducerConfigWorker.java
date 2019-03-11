package kz.greetgo.kafka2.producer;

import kz.greetgo.kafka2.core.config.ConfigEventRegistration;
import kz.greetgo.kafka2.core.config.ConfigEventType;
import kz.greetgo.kafka2.core.config.ConfigStorage;
import kz.greetgo.kafka2.util.ConfigLines;

import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class ProducerConfigWorker {
  private final Supplier<ConfigStorage> configStorage;
  private final Supplier<String> configRootPath;

  public ProducerConfigWorker(Supplier<String> configRootPath, Supplier<ConfigStorage> configStorage) {
    this.configStorage = configStorage;
    this.configRootPath = configRootPath;
  }

  private String configPath(String producerName) {
    String rootPath = configRootPath.get();
    if (rootPath == null) {
      rootPath = "";
    } else {
      rootPath = rootPath + "/";
    }
    return rootPath + producerName + ".txt";
  }

  private final ConcurrentHashMap<String, ConfigLines> configLinesMap = new ConcurrentHashMap<>();

  public Map<String, Object> getConfigFor(String producerName) {
    String configPath = configPath(producerName);

    ConfigLines configLines = configLinesMap.computeIfAbsent(configPath, this::createConfigLines);

    ConfigStorage configStorage = this.configStorage.get();

    if (!configStorage.exists(configPath)) {
      configStorage.writeContent(configPath, configLines.toBytes());
    }

    configStorage.ensureLookingFor(configPath);
    ensureRegisteredHandler();

    return configLines.getWithPrefix("prod.");
  }

  private final AtomicReference<ConfigEventRegistration> registration = new AtomicReference<>(null);

  private void ensureRegisteredHandler() {

    if (this.registration.get() == null) {
      ConfigEventRegistration registration = configStorage.get().addEventHandler(this::configEventHappened);

      if (!this.registration.compareAndSet(null, registration)) {
        registration.unregister();
      }
    }

  }

  public void close() {
    ConfigEventRegistration registration = this.registration.getAndSet(null);
    if (registration != null) {
      registration.unregister();
    }
  }

  private void configEventHappened(String path, byte[] newContent, ConfigEventType type) {
    if (type != ConfigEventType.UPDATE) {
      return;
    }

    for (String key : new ArrayList<>(configLinesMap.keySet())) {
      if (Objects.equals(key, path)) {

        ConfigLines configLines = ConfigLines.fromBytes(newContent, key);

        configLinesMap.put(key, configLines);

      }
    }
  }

  private ConfigLines createConfigLines(String configPath) {

    final byte[] configBytes;

    if (configStorage.get().exists(configPath)) {
      configBytes = configStorage.get().readContent(configPath);
    } else {
      configBytes = null;
    }

    if (configBytes == null) {
      ConfigLines ret = new ConfigLines(configPath);
      ret.putValue("prod.acts                    ", "all");
      ret.putValue("prod.buffer.memory           ", "33554432");
      ret.putValue("prod.retries                 ", "2147483647");
      ret.putValue("prod.compression.type        ", "none");
      ret.putValue("prod.batch.size              ", "16384");
      ret.putValue("prod.connections.max.idle.ms ", "540000");
      ret.putValue("prod.delivery.timeout.ms     ", "35000");
      ret.putValue("prod.request.timeout.ms      ", "30000");
      ret.putValue("prod.linger.ms               ", "1");
      ret.putValue("prod.batch.size              ", "16384");
      return ret;
    }

    return ConfigLines.fromBytes(configBytes, configPath);
  }
}
