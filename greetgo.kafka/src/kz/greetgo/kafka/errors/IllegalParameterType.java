package kz.greetgo.kafka.errors;

public class IllegalParameterType extends RuntimeException {
  public IllegalParameterType(String message) {
    super(message);
  }
}
