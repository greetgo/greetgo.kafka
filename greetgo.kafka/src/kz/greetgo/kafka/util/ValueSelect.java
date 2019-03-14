package kz.greetgo.kafka.util;

import java.util.Objects;

public class ValueSelect {

  private ValueSelect(String value, ConfigLineCommand command) {
    this.value = value;
    this.command = command;
  }

  public static ValueSelect of(String value) {
    return value == null ? new ValueSelect(null, ConfigLineCommand.NULL) : new ValueSelect(value, null);
  }

  public static ValueSelect of(ConfigLineCommand command) {
    return command == null ? new ValueSelect(null, ConfigLineCommand.NULL) : new ValueSelect(null, command);
  }

  private final String value;
  private final ConfigLineCommand command;

  public String value() {
    return value;
  }

  public ConfigLineCommand command() {
    return command;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    {
      ValueSelect that = (ValueSelect) o;
      return Objects.equals(value, that.value) && command == that.command;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, command);
  }

  @Override
  public String toString() {
    String value = this.value;
    if (value != null) {
      return "= " + value;
    }
    return ": " + command.name().toLowerCase();
  }
}
