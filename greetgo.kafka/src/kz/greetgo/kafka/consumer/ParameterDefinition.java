package kz.greetgo.kafka.consumer;

public class ParameterDefinition {
  public final String parameterName;
  public final String defaultValue;
  public final ParameterValueValidator validator;

  public ParameterDefinition(String parameterName, String defaultValue, ParameterValueValidator validator) {
    this.parameterName = parameterName;
    this.defaultValue = defaultValue;
    this.validator = validator;
  }

}
