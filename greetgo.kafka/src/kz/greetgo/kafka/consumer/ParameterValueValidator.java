package kz.greetgo.kafka.consumer;

public interface ParameterValueValidator {

  String validateValue(String value);

  int getIntValue(String value, String defaultValue);

}
