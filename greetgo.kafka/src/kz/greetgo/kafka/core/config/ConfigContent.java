package kz.greetgo.kafka.core.config;

import kz.greetgo.kafka.util.StrUtil;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toList;

public class ConfigContent {
  private final Map<String, Supplier<Optional<String>>> valueSupplierMap;
  private final List<ConfigLine> lines;
  public final byte[] contentInBytes;

  public ConfigContent(byte[] contentInBytes) {
    this(contentInBytes, null);
  }

  public ConfigContent(byte[] contentInBytes, Supplier<ConfigContent> parent) {
    this.contentInBytes = contentInBytes;

    List<ConfigLine> lines = new ArrayList<>();
    Map<String, Supplier<Optional<String>>> valueSupplierMap = new HashMap<>();

    for (String lineStr : new String(contentInBytes, StandardCharsets.UTF_8).split("\n")) {
      ConfigLine line = ConfigLine.parse(lineStr);
      lines.add(line);
      if (line.key != null) {
        valueSupplierMap.put(line.key, line.createValueSupplier(parent));
      }
    }

    this.lines = unmodifiableList(lines);
    this.valueSupplierMap = unmodifiableMap(valueSupplierMap);
  }

  public boolean parameterExists(String parameterName) {
    return valueSupplierMap.containsKey(parameterName);
  }

  public Map<String, Object> getConfigMap(String keyPrefix) {
    Map<String, Object> ret = new HashMap<>();

    List<String> filteredNames = valueSupplierMap
      .keySet()
      .stream()
      .filter(x -> x.startsWith(keyPrefix))
      .collect(toList());

    for (String filteredName : filteredNames) {
      Optional<String> optional = getStrValue(filteredName);
      if (optional.isPresent()) {

        String nameWithoutPrefix = filteredName.substring(keyPrefix.length());

        ret.put(nameWithoutPrefix, optional.get());

      }
    }

    return ret;
  }

  public Optional<Long> getLongValue(String parameterName) {
    return getStrValue(parameterName).map(StrUtil::parseLongOrNull);
  }

  public Optional<Integer> getIntValue(String parameterName) {
    return getStrValue(parameterName).map(StrUtil::parseIntOrNull);
  }

  public Optional<String> getStrValue(String parameterName) {
    if (parameterName == null) {
      return Optional.empty();
    }

    Supplier<Optional<String>> supplier = valueSupplierMap.get(parameterName);

    if (supplier == null) {
      return Optional.empty();
    }

    return supplier.get();
  }

  public byte[] generateActualValuesInBytes() {
    List<String> lines = new ArrayList<>();

    for (ConfigLine line : this.lines) {
      Optional<String> opStrValue = getStrValue(line.key);
      opStrValue.ifPresent(strValue -> lines.add(line.key + " = " + strValue));
    }

    return String.join("\n", lines).getBytes(StandardCharsets.UTF_8);
  }

  public byte[] generateErrorsInBytes() {
    List<String> lines = new ArrayList<>();

    if (!getStrValue("out.worker.count").isPresent()) {
      lines.add("");
      lines.add("ERROR: Parameter `out.worker.count` is absent - using value `0`");
    }

    int lineNumber = 1;
    for (ConfigLine line : this.lines) {

      line.appendErrorsIfExists(lines, lineNumber);

      lineNumber++;
    }

    if (lines.isEmpty()) {
      return null;
    }
    return String.join("\n", lines).getBytes(StandardCharsets.UTF_8);
  }


}
