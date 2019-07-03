package kz.greetgo.kafka.core.config;

import kz.greetgo.kafka.consumer.ConsumerConfigDefaults;
import kz.greetgo.kafka.consumer.ParameterDefinition;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

public class ConfigLine {
  private final ConsumerConfigDefaults defaults;
  public final String line;
  public final String key;
  private final String value;
  private final boolean inherits;
  public final String parseError;

  private ConfigLine(ConsumerConfigDefaults defaults,
                     String line, String key, String value, boolean inherits, String parseError) {
    this.defaults = defaults;
    this.line = line;
    this.key = key;
    this.value = value;
    this.inherits = inherits;
    this.parseError = parseError;
  }

  public static ConfigLine parse(ConsumerConfigDefaults defaults, String line) {
    String trimmed = line.trim();
    if (trimmed.isEmpty() || trimmed.startsWith("#")) {
      return new ConfigLine(defaults, line, null, null, false, null);
    }
    int idxEqual = line.indexOf('=');
    int idxComma = line.indexOf(':');
    if (idxEqual < 0 && idxComma < 0) {
      return new ConfigLine(defaults,
        line, null, null, false, "Unknown line format: must be <key> = <value> or <key> : inherits");
    }
    int idx = idxEqual < 0 ? idxComma : (idxComma < 0 ? idxEqual : Math.min(idxEqual, idxComma));
    boolean comma = idx == idxComma;
    String key = line.substring(0, idx).trim();
    String value = line.substring(idx + 1).trim();
    if (comma) {

      if ("inherits".equals(value.toLowerCase())) {
        return new ConfigLine(defaults, line, key, value, true, null);
      } else {
        return new ConfigLine(defaults, line, key, value, true, "Unknown command `" + value + "`");
      }

    }

    return new ConfigLine(defaults, line, key, value, false, null);
  }

  public Supplier<Optional<String>> createValueSupplier(Supplier<ConfigContent> parent) {
    ParameterDefinition definition = defaults.getDefinition(key);

    if (inherits) {

      if (parent == null) {
        return Optional::empty;
      }

      return () -> {
        ConfigContent parentConfigContent = parent.get();
        return parentConfigContent.getStrValue(key);
      };

    }

    {
      String validationResult = definition.validator.validateValue(value);

      if (validationResult == null) {
        return () -> Optional.of(value);
      }

      return Optional::empty;
    }
  }

  public void appendErrorsIfExists(List<String> lines, int lineNumber) {

    if (parseError != null) {
      lines.add("");
      lines.add("PARSE ERROR: line " + lineNumber + " : " + parseError + " : " + line);
      return;
    }

    if (key == null) {
      return;
    }

    if (inherits) {
      return;
    }

    ParameterDefinition definition = defaults.getDefinition(key);
    String validationError = definition.validator.validateValue(value);

    if (validationError == null) {
      return;
    }

    lines.add("");
    lines.add("ERROR: line " + lineNumber + ", parameter `" + key + "`, value `" + value + "` : " + validationError);

  }

}
