package kz.greetgo.kafka2.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;

public class ConfigLines {

  private String configPath;
  public ConfigLines parent = null;

  private final List<ConfigLine> lines = new ArrayList<>();
  private final List<String> originLines = new ArrayList<>();

  private final List<String> errors = new ArrayList<>();

  public static ConfigLines fromBytes(byte[] bytes, String configPath) {
    if (bytes == null) {
      return null;
    }
    ConfigLines ret = new ConfigLines();
    ret.configPath = configPath;
    ret.lines.addAll(
      Arrays.stream(new String(bytes, UTF_8)
        .split("\n"))
        .map(ConfigLine::parse)
        .collect(toList())
    );
    ret.originLines.clear();
    ret.originLines.addAll(ret.lines.stream().map(ConfigLine::line).collect(toList()));
    return ret;
  }

  public String getValue(String key) {

    for (ConfigLine line : lines) {

      if (line.isCommented()) {
        continue;
      }

      if (!Objects.equals(key, line.key())) {
        continue;
      }

      ConfigLineCommand command = line.command();

      if (command == null) {
        return line.value();
      }

      if (command == ConfigLineCommand.NULL) {
        return null;
      }
      if (command == ConfigLineCommand.INHERITS) {
        ConfigLines parent = this.parent;
        if (parent == null) {
          line.addError("No parent in " + configPath);
          return null;
        }
        return parent.getValue(key);
      }

      line.addError("Unknown command: " + command);

      return null;
    }

    return null;
  }

  public boolean isModified() {
    int size = originLines.size();
    if (size != lines.size()) {
      return false;
    }
    for (int i = 0; i < size; i++) {
      if (!Objects.equals(originLines.get(i), lines.get(i).line())) {
        return false;
      }
    }
    return true;
  }
}
