package kz.greetgo.kafka.core.config;

import java.util.Map;
import java.util.function.Supplier;

//TODO pompei ...
public class ConfigContent {
  public ConfigContent(byte[] contentInBytes) {
    this(contentInBytes, null);
  }

  public ConfigContent(byte[] contentInBytes, Supplier<ConfigContent> parent) {

  }

  public boolean parameterExists(String parameterName) {
    return false;
  }

  public byte[] generateErrorsInBytes() {
    return new byte[0];
  }

  public Map<String, Object> getConfigMap(String keyPrefix) {
    return null;
  }

  public int getLongValue(String parameterName) {
    return 0;
  }

  public byte[] generateActualValuesInBytes() {
    return new byte[0];
  }
}
