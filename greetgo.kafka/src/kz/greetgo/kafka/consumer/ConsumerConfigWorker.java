package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.core.config.EventConfigFile;
import kz.greetgo.kafka.core.config.EventConfigFileFromStorage;
import kz.greetgo.kafka.core.config.EventConfigStorage;
import kz.greetgo.kafka.util.Handler;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class ConsumerConfigWorker implements AutoCloseable {
  private final Supplier<EventConfigStorage> configStorage;
  private final Handler configDataChanged;
  private final Supplier<ConsumerConfigDefaults> defaults;
  private String configPathPrefix;
  private String hostId;

  public void setConfigPathPrefix(String configPathPrefix) {
    checkNotStarted();
    this.configPathPrefix = configPathPrefix;
  }

  public void setHostId(String hostId) {
    checkNotStarted();
    this.hostId = hostId;
  }

  private void checkNotStarted() {
    if (worker.get() != null) {
      throw new RuntimeException("You cannot do it after worker has been started");
    }
  }

  public ConsumerConfigWorker(Supplier<EventConfigStorage> configStorage,
                              Handler configDataChanged,
                              Supplier<ConsumerConfigDefaults> defaults) {
    this.configStorage = configStorage;
    this.configDataChanged = configDataChanged;
    this.defaults = defaults;
  }

  private final AtomicReference<ConsumerConfigFileWorker> worker = new AtomicReference<>(null);

  public void start() {
    if (worker.get() != null) {
      return;
    }

    if (hostId == null) {
      throw new RuntimeException("Not defined hostId");
    }

    EventConfigFile parentConfig = eventConfigFileOn(configPathPrefix + ".conf");
    EventConfigFile parentConfigError = eventConfigFileOn(configPathPrefix + ".errors");
    EventConfigFile hostConfig = eventConfigFileOn(configPathPrefix + ".d/" + hostId + ".conf");
    EventConfigFile hostConfigError = eventConfigFileOn(configPathPrefix + ".d/" + hostId + ".errors");
    EventConfigFile hostConfigActualValues = eventConfigFileOn(configPathPrefix + ".d/" + hostId + ".actual-values");

    ConsumerConfigFileWorker worker2 = new ConsumerConfigFileWorker(
      configDataChanged,

      parentConfig,
      parentConfigError,
      hostConfig,
      hostConfigError,
      hostConfigActualValues,

      defaults

    );

    worker2.start();

    if (!worker.compareAndSet(null, worker2)) {
      worker2.close();
    }
  }

  private EventConfigFile eventConfigFileOn(String configPath) {
    return new EventConfigFileFromStorage(configPath, configStorage.get());
  }

  private ConsumerConfigFileWorker worker() {
    ConsumerConfigFileWorker ret = worker.get();
    if (ret == null) {
      throw new RuntimeException("You must start worker");
    }
    return ret;
  }

  @Override
  public void close() {
    ConsumerConfigFileWorker worker2 = worker.get();
    if (worker2 != null) {
      worker2.close();
    }
  }

  /**
   * @return Количество необходимых воркеров. 0 - консюмер не работает. 1 - значение по-умолчанию
   */
  public int getWorkerCount() {
    return worker().getWorkerCount();
  }

  public Map<String, Object> getConfigMap() {
    return worker().getConfigMap();
  }

  public Duration pollDuration() {
    return worker().pollDuration();
  }
}
