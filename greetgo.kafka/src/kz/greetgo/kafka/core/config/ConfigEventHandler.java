package kz.greetgo.kafka.core.config;

public interface ConfigEventHandler {
  void configEventHappened(String path, ConfigEventType type);
}
