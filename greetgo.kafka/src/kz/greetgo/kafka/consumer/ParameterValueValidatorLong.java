package kz.greetgo.kafka.consumer;

public class ParameterValueValidatorLong implements ParameterValueValidator {

  @Override
  public String validateValue(String value) {
    try {
      Long.parseLong(value);
      return null;
    } catch (RuntimeException ignore) {
      return "value must be integer";
    }
  }

  @Override
  public int getIntValue(String value, String defaultValue) {
    throw new RuntimeException("This is not int parameter");
  }

}
