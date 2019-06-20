package kz.greetgo.kafka.core.config;

import java.util.Date;
import java.util.Optional;

public class EventConfigStorageUtil {

  private static class ChangedRoot implements EventConfigStorage {

    private final String upPath;
    private final EventConfigStorage parent;

    public ChangedRoot(String upPath, EventConfigStorage parent) {
      this.upPath = upPath;
      this.parent = parent;
    }

    private String changePath(String path) {
      if (path == null || path.length() == 0) {
        return upPath;
      }
      return upPath + "/" + path;
    }

    @Override
    public Optional<Date> createdAt(String path) {
      return parent.createdAt(changePath(path));
    }

    @Override
    public Optional<Date> lastModifiedAt(String path) {
      return parent.lastModifiedAt(changePath(path));
    }

    @Override
    public byte[] readContent(String path) {
      return parent.readContent(changePath(path));
    }

    @Override
    public void writeContent(String path, byte[] content) {
      parent.writeContent(changePath(path), content);
    }

    @Override
    public void ensureLookingFor(String path) {
      parent.ensureLookingFor(changePath(path));
    }

    @Override
    @SuppressWarnings("Convert2Lambda")
    public ConfigEventRegistration addEventHandler(ConfigEventHandler configEventHandler) {
      return parent.addEventHandler(new ConfigEventHandler() {
        @Override
        public void configEventHappened(String path, ConfigEventType type) {
          if (path.startsWith(upPath + "/")) {
            configEventHandler.configEventHappened(path.substring(upPath.length() + 1), type);
          }
        }
      });
    }
  }

  public static EventConfigStorage changeRoot(String upPath, EventConfigStorage parent) {
    return new ChangedRoot(upPath, parent);
  }

}
