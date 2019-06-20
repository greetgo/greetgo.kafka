package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.core.config.EventConfigStorage;
import kz.greetgo.kafka.util.Handler;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class ConsumerConfigWorker implements AutoCloseable {
  private final Supplier<EventConfigStorage> configStorage;
  private final Handler configDataChanged;
  private String configPath;
  private String hostName;

  public void setConfigPathPrefix(String configPath) {
    checkNotStarted();
    this.configPath = configPath;
  }

  private void checkNotStarted() {
    if (worker.get() != null) {
      throw new RuntimeException("You cannot do it after worker has been started");
    }
  }

  public void setHostId(String hostName) {
    this.hostName = hostName;
    checkNotStarted();
  }

  public ConsumerConfigWorker(Supplier<EventConfigStorage> configStorage, Handler configDataChanged) {
    this.configStorage = configStorage;
    this.configDataChanged = configDataChanged;
  }

  private final AtomicReference<ConsumerConfigWorker2> worker = new AtomicReference<>(null);

  public void start() {
    if (worker.get() != null) {
      return;
    }

    ConsumerConfigWorker2 worker2 = new ConsumerConfigWorker2(
      // TODO pompei ...
    );

    if (!worker.compareAndSet(null, worker2)) {
      worker2.close();
    }
  }

  private ConsumerConfigWorker2 worker() {
    ConsumerConfigWorker2 ret = worker.get();
    if (ret == null) {
      throw new RuntimeException("You must start worker");
    }
    return ret;
  }

  @Override
  public void close() {
    ConsumerConfigWorker2 worker2 = worker.get();
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
