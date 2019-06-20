package kz.greetgo.kafka.core.config;

import java.util.Date;
import java.util.Optional;

public interface EventConfigFile extends AutoCloseable {

  EventRegistration addEventHandler(EventFileHandler eventFileHandler);

  default boolean exists() {
    return createdAt().isPresent();
  }

  void ensureLookingFor();

  Optional<Date> createdAt();

  Optional<Date> lastModifiedAt();

  byte[] readContent();

  void writeContent(byte[] content);

  default void delete(String path) {
    writeContent(null);
  }

  @Override
  void close();

}
