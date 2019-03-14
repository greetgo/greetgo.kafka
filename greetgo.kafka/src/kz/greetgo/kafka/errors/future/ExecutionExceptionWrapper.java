package kz.greetgo.kafka.errors.future;

import java.util.concurrent.ExecutionException;

public class ExecutionExceptionWrapper extends RuntimeException {
  public ExecutionExceptionWrapper(ExecutionException e) {
    super(e.getMessage(), e);
  }
}
