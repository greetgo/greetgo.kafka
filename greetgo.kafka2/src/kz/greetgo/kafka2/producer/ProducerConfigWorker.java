package kz.greetgo.kafka2.producer;

import kz.greetgo.kafka2.core.config.ConfigStorage;
import kz.greetgo.kafka2.util.ConfigLines;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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

  private void ensureRegisteredHandler() {

  }

  private ConfigLines createConfigLines(String configPath) {
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
}
