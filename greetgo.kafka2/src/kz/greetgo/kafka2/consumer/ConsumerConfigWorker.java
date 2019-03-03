package kz.greetgo.kafka2.consumer;

import kz.greetgo.kafka2.core.config.ConfigEventRegistration;
import kz.greetgo.kafka2.core.config.ConfigEventType;
import kz.greetgo.kafka2.core.config.ConfigStorage;
import kz.greetgo.kafka2.util.Handler;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class ConsumerConfigWorker implements AutoCloseable {
  private Supplier<ConfigStorage> configStorage;
  private Handler configDataChanged;
  private final AtomicReference<ConfigEventRegistration> configEventRegistration = new AtomicReference<>(null);

  public final AtomicReference<String> configPath = new AtomicReference<>(null);


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

  public List<String> topicList() {
    throw new RuntimeException("Реализовать");
  }

  public Duration pollDuration() {
    return Duration.ofMillis(1000);
  }
}
