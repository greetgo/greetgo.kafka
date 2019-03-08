package kz.greetgo.kafka2.util;

import java.util.ArrayList;
import java.util.List;

public class ConfigLine {

  public static ConfigLine parse(String line) {
    if (line == null) {
      return null;
    }
    return new ConfigLine(line);
  }

  private final List<String> errors = new ArrayList<>();
  private ConfigLineKey key;
  private ConfigLineValue value;
  private String line = null;

  private ConfigLine() {}

  public ConfigLine copy() {
    ConfigLine ret = new ConfigLine();
    ret.key = key == null ? null : key.copy();
    ret.value = value == null ? null : value.copy();
    ret.line = line;
    ret.errors.addAll(errors);
    return ret;
  }

  public String line() {
    return line != null ? line : (key == null || value == null ? "" : key.toString() + value.toString());
  }

  private ConfigLine(String line) {

    String trimmedLine = line.trim();

    if (trimmedLine.isEmpty() || trimmedLine.startsWith("##")) {
      this.line = line;
      key = null;
      value = null;
      return;
    }

    key = ConfigLineKey.parse(line);
    value = ConfigLineValue.parse(line);

    if (key == null || value == null) {
      key = null;
      value = null;
      this.line = line;
      errors.add("Unknown format of line `" + line + "`");
    }

  }

  public boolean isCommented() {
    return key == null || key.isCommented();
  }

  public String key() {
    return key == null ? null : key.key();
  }

  public ConfigLineCommand command() {
    return value == null ? null : value.command();
  }

  public String value() {
    return value == null ? null : value.value();
  }

  public void addError(String message) {
    errors.add(message);
  }

  public String keyPart() {
    return key == null ? null : key.toString();
  }

  public String valuePart() {
    return value == null ? null : value.toString();
  }

  public List<String> errors() {
    List<String> ret = new ArrayList<>();
    ConfigLineValue value = this.value;
    if (value != null) {
      ret.addAll(value.errors());
    }
    ret.addAll(errors);
    return ret;
  }

  public void setValue(String newValue) {
    if (value == null) {
      throw new IllegalStateException("value == null in method ConfigLine.setValue()");
    }

    value.setValue(newValue);
  }

  public void setCommented(boolean commented) {
    if (key == null) {
      throw new IllegalStateException("key == null in method ConfigLine.setCommented()");
    }

    key.setCommented(commented);

  }

  public void setCommand(ConfigLineCommand command) {
    if (command == null) {
      throw new IllegalArgumentException("command == null in method ConfigLine.setCommand()");
    }
    if (value == null) {
      throw new IllegalStateException("value == null in method ConfigLine.setCommand()");
    }
    value.setCommand(command);
  }

  public void setKey(String key) {
    if (this.key == null) {
      String line = "#" + key + " : null";
      this.line = null;
      this.key = ConfigLineKey.parse(line);
      this.value = ConfigLineValue.parse(line);
      return;
    }
    this.key.setKey(key);
  }

  public boolean isValueEqualTo(String value) {
    if (this.value == null) {
      return false;
    }
    return this.value.isValueEqualTo(value);
  }

  public boolean isEqualTo(ValueSelect valueSelect) {
    if (valueSelect == null) {
      return false;
    }

    return valueSelect.equals(get());
  }

  @Override
  public String toString() {
    return line();
  }

  public void set(ValueSelect valueSelect) {
    if (valueSelect == null) {
      throw new IllegalArgumentException("valueSelect == null");
    }

    String value = valueSelect.value();
    if (value != null) {
      setValue(value);
      return;
    }

    ConfigLineCommand command = valueSelect.command();
    if (command != null) {
      setCommand(command);
    }
  }

  public ValueSelect get() {
    {
      String value = value();
      if (value != null) {
        return ValueSelect.of(value);
      }
    }

    {
      ConfigLineCommand command = command();
      if (command != null) {
        return ValueSelect.of(command);
      }
    }

    throw new IllegalStateException("ConfigLineValue.value() == null && ConfigLineValue.command() == null");
  }
}
