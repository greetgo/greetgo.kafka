package kz.greetgo.kafka2.util;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static kz.greetgo.kafka2.util.StrUtil.expandStr;
import static kz.greetgo.kafka2.util.StrUtil.spaces;
import static kz.greetgo.kafka2.util.StrUtil.startSpaces;

public class ConfigLine {

  public static ConfigLine parse(String line) {
    return new ConfigLine(line);
  }

  private final List<String> errors = new ArrayList<>();
  private String key;
  private String keyPart;
  private String value;
  private String valuePart;
  private ConfigLineCommand command;
  private String line = null;
  private boolean commented;

  private ConfigLine() {}

  public ConfigLine copy() {
    ConfigLine ret = new ConfigLine();
    ret.key = key;
    ret.keyPart = keyPart;
    ret.value = value;
    ret.valuePart = valuePart;
    ret.command = command;
    ret.line = line;
    ret.commented = commented;
    ret.errors.addAll(errors);
    return ret;
  }

  public String line() {
    return line != null ? line : keyPart + valuePart;
  }

  private ConfigLine(String line) {

    String trimmedLine = line.trim();

    if (trimmedLine.isEmpty() || trimmedLine.startsWith("##")) {
      this.line = line;
      key = null;
      keyPart = null;
      value = null;
      valuePart = null;
      command = null;
      commented = !trimmedLine.isEmpty();
      return;
    }

    commented = trimmedLine.startsWith("#");

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
    keyPart = line.substring(0, index);
    valuePart = line.substring(index);

    if (key.startsWith("#")) {
      key = key.substring(1).trim();
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
    return value;
  }

  public void addError(String message) {
    errors.add(message);
  }

  public String keyPart() {
    return keyPart;
  }

  public String valuePart() {
    return valuePart;
  }

  public List<String> errors() {
    return unmodifiableList(errors);
  }

  public void setValue(String newValue) {
    if (isValueEqualsTo(newValue)) {
      return;
    }

    if (newValue == null) {
      setCommand(ConfigLineCommand.NULL);
      return;
    }

    int startSpaces = startSpaces(valuePart.substring(1));

    int valueSpace = valuePart.length() - startSpaces - 1;

    valuePart = "=" + spaces(startSpaces) + expandStr(newValue, valueSpace);
    value = newValue;

    command = null;

    checkState();
  }

  private void checkState() {
    if (value == null && command == null) {
      throw new IllegalStateException("value == null && command == null");
    }
    if (value != null && command != null) {
      throw new IllegalStateException("value == `" + value + "` && command == " + command);
    }
  }

  public void setCommented(boolean commented) {
    if (this.commented == commented) {
      return;
    }
    this.commented = commented;

    int index = -1;
    int count = 0;
    for (int i = 0; i < keyPart.length(); i++) {
      char c = keyPart.charAt(i);
      if (c == '#') {
        index = i;
        count++;
        continue;
      }
      if (Character.isWhitespace(c)) {
        count++;
        continue;
      }
      break;
    }

    if (index < 0 && !commented) {
      return;
    }
    if (index >= 0 && commented) {
      return;
    }

    if (!commented) {
      keyPart = keyPart.substring(0, index) + ' ' + keyPart.substring(index + 1);
    } else {

      if (count > 0) {
        keyPart = "#" + keyPart.substring(1);
      } else {
        keyPart = "#" + keyPart;
      }

    }

    checkState();
  }

  public void setCommand(ConfigLineCommand command) {
    if (command == null) {
      throw new IllegalArgumentException("command == null");
    }

    if (this.command == command) {
      return;
    }

    this.command = command;
    this.value = null;

    int startSpaceCount = 0;

    if (valuePart != null) {
      char[] chars = valuePart.substring(1).toCharArray();
      for (int i = 0; i < chars.length; i++) {
        if (!Character.isWhitespace(chars[i])) {
          startSpaceCount = i;
          break;
        }
      }
    }

    valuePart = ":" + StrUtil.spaces(startSpaceCount) + command.name().toLowerCase();

    if (command == ConfigLineCommand.UNKNOWN) {
      addError("Command is UNKNOWN");
    }

    checkState();
  }

  public void setKey(String key) {

    if (keyPart == null) {
      this.key = key;
      keyPart = "#" + key + " ";
      commented = true;
      valuePart = ": null";
      command = ConfigLineCommand.NULL;
      line = null;
      checkState();
      return;
    }

    if (keyPart.trim().startsWith("#")) {

      int sharpIndex = keyPart.indexOf('#');
      int keyStartIndex = -1;

      for (int i = sharpIndex + 1; i < keyPart.length(); i++) {
        if (!Character.isWhitespace(keyPart.charAt(i))) {
          keyStartIndex = i;
          break;
        }
      }

      {
        StringBuilder sb = new StringBuilder();
        sb.append(spaces(sharpIndex)).append('#');
        if (keyStartIndex >= 0) {
          sb.append(spaces(keyStartIndex - sharpIndex - 1));
        }
        sb.append(key);
        while (sb.length() < keyPart.length()) {
          sb.append(' ');
        }

        keyPart = sb.toString();
        this.key = key;
        commented = true;
      }

      checkState();
      return;
    }

    {
      int keyStartIndex = -1;

      for (int i = 0; i < keyPart.length(); i++) {
        if (!Character.isWhitespace(keyPart.charAt(i))) {
          keyStartIndex = i;
          break;
        }
      }

      StringBuilder sb = new StringBuilder();
      sb.append(spaces(keyStartIndex)).append(key);
      while (sb.length() < keyPart.length()) {
        sb.append(' ');
      }

      keyPart = sb.toString();
      this.key = key;
      commented = false;

      checkState();
    }
  }

  public boolean isValueEqualsTo(String value) {
    if (command == ConfigLineCommand.NULL) {
      return value == null;
    }
    return this.value.equals(value);
  }

  @Override
  public String toString() {
    return line();
  }
}
