package kz.greetgo.kafka.core.config;

import java.util.Date;
import java.util.Optional;

/**
 * Interface to access to file storage
 */
public interface EventConfigStorage extends AutoCloseable {

  /**
   * Checks file to existence
   *
   * @param path path to file
   * @return existence info: true - exists, otherwise - no
   */
  default boolean exists(String path) {
    return createdAt(path).isPresent();
  }

  /**
   * Returns creation time
   *
   * @param path path to file
   * @return creation time optional. If it is empty, then file is absent.
   */
  Optional<Date> createdAt(String path);

  /**
   * Returns last modification time
   *
   * @param path path to file
   * @return last modification time optional. If it is empty, then file is absent.
   */
  Optional<Date> lastModifiedAt(String path);

  /**
   * Reads content of file
   *
   * @param path path to file
   * @return content of file or null if file is absent
   */
  byte[] readContent(String path);

  /**
   * Writes content of file. Path o file creates automatically.
   *
   * @param path    path to file
   * @param content written content. If <code>null</code> then deletes the file
   */
  void writeContent(String path, byte[] content);

  /**
   * Deletes the file
   *
   * @param path path to file
   */
  default void delete(String path) {
    writeContent(path, null);
  }

  /**
   * Setup, if absent, looker to file. File must exists. If file is absent, throws an error.
   *
   * @param path path to file
   */
  void ensureLookingFor(String path);

  /**
   * Adds event handler for all looking files
   *
   * @param configEventHandler handler
   * @return registration object using to unregister handler
   */
  EventRegistration addEventHandler(ConfigEventHandler configEventHandler);

  @Override
  void close();

}
