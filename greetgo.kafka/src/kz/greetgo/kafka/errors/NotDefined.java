package kz.greetgo.kafka.errors;

public class NotDefined extends RuntimeException {
  public NotDefined(String message) {
    super(message);
  }
}
