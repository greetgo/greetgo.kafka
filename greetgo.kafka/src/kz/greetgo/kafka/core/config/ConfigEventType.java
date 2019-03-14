package kz.greetgo.kafka.core.config;

public enum ConfigEventType {
  /**
   * Файл конфига только что создан
   */
  CREATE,

  /**
   * Файл конфига только что изменён
   */
  UPDATE,

  /**
   * Файл конфига только что удалён
   */
  DELETE,
}
