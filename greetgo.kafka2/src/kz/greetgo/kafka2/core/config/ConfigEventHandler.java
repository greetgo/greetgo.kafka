package kz.greetgo.kafka2.core.config;

public interface ConfigEventHandler {
  void configEventHappened(String path, byte[] newContent, ConfigEventType type);
}
