package kz.greetgo.kafka_old.producer;

public class RuntimeInterruptedException extends RuntimeException {
  public RuntimeInterruptedException(InterruptedException e) {
    super(e);
  }
}
