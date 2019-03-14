package kz.greetgo.kafka.producer;

public class RuntimeInterruptedException extends RuntimeException {
  public RuntimeInterruptedException(InterruptedException e) {
    super(e);
  }
}
