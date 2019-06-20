package kz.greetgo.kafka.consumer;

public class ParameterValueValidatorInt implements ParameterValueValidator {

  @Override
  public String validateValue(String value) {
    try {
      Integer.parseInt(value);
      return null;
    } catch (RuntimeException ignore) {
      return "value must be integer";
    }
  }

  @Override
  public int getIntValue(String value, String defaultValue) {
    try {
      return Integer.parseInt(value);
    } catch (RuntimeException ignore) {
      return Integer.parseInt(defaultValue);
    }
  }

}
