package kz.greetgo.kafka.errors;

public class ConsumerInvocationException extends RuntimeException {
  public ConsumerInvocationException(String message, Throwable cause) {
    super(message, cause);
  }
}
