package kz.greetgo.kafka2.errors;

public class IllegalParameterType extends RuntimeException {
  public IllegalParameterType(String message) {
    super(message);
  }
}
