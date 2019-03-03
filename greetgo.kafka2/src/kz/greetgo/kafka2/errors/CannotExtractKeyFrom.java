package kz.greetgo.kafka2.errors;

public class CannotExtractKeyFrom extends RuntimeException {
  public CannotExtractKeyFrom(Object object) {
    super(object == null ? "object == null" : "object.class = " + object.getClass());
  }
}
