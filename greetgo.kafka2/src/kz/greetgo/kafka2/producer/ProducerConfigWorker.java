package kz.greetgo.kafka2.producer;

import kz.greetgo.kafka2.core.config.ConfigStorage;

import java.util.Map;
import java.util.function.Supplier;

public class ProducerConfigWorker {
  private final Supplier<ConfigStorage> configStorage;

  public ProducerConfigWorker(Supplier<ConfigStorage> configStorage) {
    this.configStorage = configStorage;
  }

  public Map<String, Object> getConfigFor(String producerName) {
    throw new RuntimeException("Сделать");
  }
}
