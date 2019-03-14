package kz.greetgo.kafka.core.config;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

public class ConfigStorageInDir extends ConfigStorageAbstract {

  private final Path rootDir;

  public ConfigStorageInDir(Path rootDir) {
    this.rootDir = rootDir;
  }

  @Override
  public boolean exists(String path) {
    return getFile(path).exists();
  }

  private void putToCurrentState(String path, byte[] bytes) {
    if (bytes == null) {
      currentState.remove(path);
    } else {
      currentState.put(path, bytes);
    }
  }

  @Override
  public byte[] readContent(String path) {
    File file = getFile(path);
    if (!file.exists()) {
      if (lookingFor.containsKey(path)) {
        putToCurrentState(path, null);
      }
      return null;
    }
    try {
      byte[] bytes = Files.readAllBytes(file.toPath());
      if (lookingFor.containsKey(path)) {
        putToCurrentState(path, bytes);
      }
      return bytes;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private File getFile(String path) {
    return rootDir.resolve(path).toFile();
  }

  @Override
  public void writeContent(String path, byte[] content) {
    File file = getFile(path);
    if (content == null) {
      if (file.exists()) {
        //noinspection ResultOfMethodCallIgnored
        file.delete();
      }
      return;
    }

    //noinspection ResultOfMethodCallIgnored
    file.getParentFile().mkdirs();

    try {
      Files.write(file.toPath(), content);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private final ConcurrentHashMap<String, byte[]> currentState = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Boolean> lookingFor = new ConcurrentHashMap<>();

  @Override
  public void ensureLookingFor(String path) {
    lookingFor.put(path, true);
  }

  public void idle() {
    new ArrayList<>(lookingFor.keySet()).forEach(this::idleOnPath);
  }

  private void idleOnPath(String path) {
    byte[] current = currentState.get(path);
    byte[] real = readContent(path);

    if (Arrays.equals(current, real)) {
      return;
    }

    if (real == null) {
      fireConfigEventHandler(path, ConfigEventType.DELETE);
      return;
    }

    if (current == null) {
      fireConfigEventHandler(path, ConfigEventType.CREATE);
      return;
    }

    fireConfigEventHandler(path, ConfigEventType.UPDATE);
  }
}
