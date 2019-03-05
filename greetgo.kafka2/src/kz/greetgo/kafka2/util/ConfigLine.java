package kz.greetgo.kafka2.util;

import java.util.ArrayList;
import java.util.List;

public class ConfigLine {

  public static ConfigLine parse(String line) {
    return new ConfigLine(line);
  }

  private final List<String> errors = new ArrayList<>();

  public String line() {
    if (line != null) {
      return line;
    }


    return  key;
  }

  private String key;
  private String value;
  private ConfigLineCommand command;
  private String line = null;

  private ConfigLine(String line) {

    if (line.trim().startsWith("##")) {
      this.line = line;
      key = null;
      value = null;
      command = null;
      commented = true;
      return;
    }

    commented = line.trim().startsWith("#");

    int index = line.indexOf('=');
    int ppIndex = line.indexOf(':');

    if (ppIndex >= 0) {
      index = index < 0 ? ppIndex : Math.min(ppIndex, index);
    }

    if (index < 0) {
      this.line = line;
      if (!commented) {
        addError("Unknown line format");
      }
      return;
    }

    this.key = null;
    this.value = null;
    this.command = null;

    String key = line.substring(0, index).trim();
    String type = line.substring(index, index + 1);
    String value = line.substring(index + 1).trim();

    if (key.startsWith("#")) {
      key = key.substring(1);
    }

    this.key = key;

    if (":".equals(type)) {
      command = ConfigLineCommand.valueOrUnknown(value);
      if (command == ConfigLineCommand.UNKNOWN) {
        addError("Unknown command " + value);
        this.line = line;
      }
      return;
    }

    this.value = value;
  }

  private boolean commented;

  public boolean isCommented() {
    return commented;
  }

  public String key() {
    return key;
  }

  public ConfigLineCommand command() {
    return command;
  }

  public String value() {
    return command == null ? value : null;
  }

  public void addError(String message) {
    errors.add(message);
  }
}
