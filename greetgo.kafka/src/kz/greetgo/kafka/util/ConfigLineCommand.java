package kz.greetgo.kafka.util;

public enum ConfigLineCommand {
  UNKNOWN, NULL, INHERITS;

  public static ConfigLineCommand valueOrUnknown(String str) {
    if (str == null) {
      return UNKNOWN;
    }

    String STR = str.trim().toUpperCase();

    for (ConfigLineCommand value : values()) {
      if (value.name().toUpperCase().equals(STR)) {
        return value;
      }
    }

    return UNKNOWN;
  }
}
