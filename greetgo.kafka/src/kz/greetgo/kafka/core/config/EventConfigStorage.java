package kz.greetgo.kafka.core.config;

/**
 * Interface to access to file storage
 */
public interface EventConfigStorage {

  /**
   * Checks file to existence
   *
   * @param path path to file
   * @return existence info: true - exists, otherwise - no
   */
  boolean exists(String path);

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
   * @param content written content
   */
  void writeContent(String path, byte[] content);

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
  ConfigEventRegistration addEventHandler(ConfigEventHandler configEventHandler);
}
