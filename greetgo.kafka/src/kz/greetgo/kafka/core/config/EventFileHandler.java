package kz.greetgo.kafka.core.config;

public interface EventFileHandler {
  void eventHappened(ConfigEventType type);
}
