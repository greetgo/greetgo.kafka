package kz.greetgo.kafka2.util;

import java.util.ArrayList;
import java.util.List;

import static kz.greetgo.kafka2.util.StrUtil.spaces;
import static kz.greetgo.kafka2.util.StrUtil.startSpacesCount;

public class ConfigLineValue {

  private final List<String> errors = new ArrayList<>();

  public static ConfigLineValue parseLine(String line) {

    if (line == null) {
      return null;
    }

    if (line.trim().startsWith("##")) {
      return null;
    }

    int index = StrUtil.firstIndexOf(line, '=', ':');
    if (index < 0) {
      return null;
    }

    return new ConfigLineValue(line.substring(index));

  }

  private ConfigLineValue(String valuePart) {

    char firstChar = valuePart.charAt(0);

    String text = valuePart.substring(1);

    paddingLeft = startSpacesCount(text);
    width = text.length() - paddingLeft;
    if (width == 0 && paddingLeft > 0) {
      width = paddingLeft - 1;
      paddingLeft = 1;
    }

    if (firstChar == '=') {
      value = text.trim();
      command = null;
      return;
    }

    if (firstChar == ':') {

      command = text.trim();

      if (ConfigLineCommand.valueOrUnknown(command) == ConfigLineCommand.UNKNOWN) {
        errors.add("Unknown command `" + command + "`");
      }

      return;
    }

    throw new IllegalArgumentException("Unknown firstChar = " + firstChar);
  }


  private String value;
  private int paddingLeft;
  private int width;
  private String command;

  public String value() {
    return value;
  }

  public ConfigLineCommand command() {
    return command == null ? null : ConfigLineCommand.valueOrUnknown(command);
  }

  public int paddingLeft() {
    return paddingLeft;
  }

  public int width() {
    return width;
  }

  public void setValue(String value) {
    if (value == null) {
      command = ConfigLineCommand.NULL.name().toLowerCase();
      this.value = null;
    } else {
      this.value = value;
      command = null;
    }

    updateWidth();
  }

  private void updateWidth() {
    if (command != null) {
      width = Math.max(width, command.length());
    }

    if (value != null) {
      width = Math.max(width, value.length());
    }
  }

  public List<String> errors() {
    return errors;
  }

  public void setCommand(ConfigLineCommand command) {
    if (command == null) {
      command = ConfigLineCommand.NULL;
    }

    this.command = command.name().toLowerCase();
    value = null;
    updateWidth();
  }

  @Override
  public String toString() {

    StringBuilder sb = new StringBuilder(paddingLeft + width + 10);
    sb.append(command == null ? '=' : ':');
    sb.append(spaces(paddingLeft));
    int beforeLength = sb.length();
    if (command != null) {
      sb.append(command);
    }
    if (value != null) {
      sb.append(value);
    }

    int needLength = beforeLength + width;

    while (sb.length() < needLength) {
      sb.append(' ');
    }

    return sb.toString();
  }
}
