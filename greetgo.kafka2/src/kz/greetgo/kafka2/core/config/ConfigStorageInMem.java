package kz.greetgo.kafka2.core.config;

import java.util.concurrent.ConcurrentHashMap;

public class ConfigStorageInMem extends ConfigStorageAbstract {

  private final ConcurrentHashMap<String, byte[]> data = new ConcurrentHashMap<>();

  @Override
  public boolean exists(String path) {
    return data.containsKey(path);
  }

  @Override
  public byte[] readContent(String path) {
    return data.get(path);
  }

  @Override
  public void writeContent(String path, byte[] content) {
    if (content == null) {
      data.remove(path);
    } else {
      data.put(path, content);
    }
  }

  @Override
  public void ensureLookingFor(String path) {}
}
