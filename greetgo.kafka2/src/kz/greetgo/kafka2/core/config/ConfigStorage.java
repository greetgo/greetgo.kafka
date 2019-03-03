package kz.greetgo.kafka2.core.config;

public interface ConfigStorage {

  boolean exists(String path);

  byte[] readContent(String path);

  void writeContent(String path, byte[] content);

  ConfigEventRegistration addEventHandler(ConfigEventHandler configEventHandler);
}
