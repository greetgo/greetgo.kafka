package kz.greetgo.kafka2.errors.future;

public class InterruptedExceptionWrapper extends RuntimeException {
  public InterruptedExceptionWrapper(InterruptedException e) {
    super(e.getMessage(), e);
  }
}
