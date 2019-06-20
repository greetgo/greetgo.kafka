package kz.greetgo.kafka.consumer;

public class ParameterValueValidatorStr implements ParameterValueValidator {

  @Override
  public String validateValue(String value) {
    return null;
  }

  @Override
  public int getIntValue(String value, String defaultValue) {
    throw new RuntimeException("This is not int parameter");
  }
}
