package kz.greetgo.kafka.core.config;

import java.util.Date;
import java.util.Objects;
import java.util.Optional;

public class EventConfigFileFromStorage implements EventConfigFile {

  private final String path;
  private final EventConfigStorage storage;

  public EventConfigFileFromStorage(String path, EventConfigStorage storage) {
    this.path = path;
    this.storage = storage;
  }

  @Override
  public EventRegistration addEventHandler(EventFileHandler eventFileHandler) {
    return storage.addEventHandler((pathArg, type) -> {
      if (Objects.equals(pathArg, path)) {
        eventFileHandler.eventHappened(type);
      }
    });
  }

  @Override
  public void ensureLookingFor() {
    storage.ensureLookingFor(path);
  }

  @Override
  public Optional<Date> createdAt() {
    return storage.createdAt(path);
  }

  @Override
  public Optional<Date> lastModifiedAt() {
    return storage.lastModifiedAt(path);
  }

  @Override
  public byte[] readContent() {
    return storage.readContent(path);
  }

  @Override
  public void writeContent(byte[] content) {
    storage.writeContent(path, content);
  }

  @Override
  public void close() {
    //nothing to do
  }

}
