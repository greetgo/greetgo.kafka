package kz.greetgo.kafka.errors;

public class CannotExtractKeyFrom extends RuntimeException {
  public CannotExtractKeyFrom(Object object) {
    super(object == null ? "object == null" : "object.class = " + object.getClass());
  }
}
