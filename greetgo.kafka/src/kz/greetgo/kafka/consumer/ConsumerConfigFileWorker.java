package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.core.config.EventConfigFile;
import kz.greetgo.kafka.util.Handler;

import java.time.Duration;
import java.util.Map;

public class ConsumerConfigFileWorker {

  private final Handler configDataChanged;
  private final EventConfigFile parentConfig;
  private final EventConfigFile parentConfigError;
  private final EventConfigFile hostConfig;
  private final EventConfigFile hostConfigError;
  private final EventConfigFile hostConfigActualValues;

  public ConsumerConfigFileWorker(Handler configDataChanged,
                                  EventConfigFile parentConfig, EventConfigFile parentConfigError,
                                  EventConfigFile hostConfig, EventConfigFile hostConfigError,
                                  EventConfigFile hostConfigActualValues) {

    this.configDataChanged = configDataChanged;
    this.parentConfig = parentConfig;
    this.parentConfigError = parentConfigError;
    this.hostConfig = hostConfig;
    this.hostConfigError = hostConfigError;
    this.hostConfigActualValues = hostConfigActualValues;
  }

  public void start() {

  }

  public int getWorkerCount() {
    return 0;
  }

  public Map<String, Object> getConfigMap() {
    return null;
  }

  public Duration pollDuration() {
    return null;
  }

  public void close() {

  }
}
