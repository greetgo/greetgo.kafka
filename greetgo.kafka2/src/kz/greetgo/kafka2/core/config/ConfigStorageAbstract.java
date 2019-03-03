package kz.greetgo.kafka2.core.config;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public abstract class ConfigStorageAbstract implements ConfigStorage {

  private final ConcurrentHashMap<Long, ConfigEventHandler> map = new ConcurrentHashMap<>();

  private final AtomicLong nextId = new AtomicLong(1);

  @Override
  public ConfigEventRegistration addEventHandler(ConfigEventHandler configEventHandler) {
    final long id = nextId.getAndIncrement();
    map.put(id, configEventHandler);
    return () -> map.remove(id);
  }

  protected void fireConfigEventHandler(String path, byte[] newContent, ConfigEventType type) {
    for (ConfigEventHandler configEventHandler : new ArrayList<>(map.values())) {
      configEventHandler.configEventHappened(path, newContent, type);
    }
  }
}
