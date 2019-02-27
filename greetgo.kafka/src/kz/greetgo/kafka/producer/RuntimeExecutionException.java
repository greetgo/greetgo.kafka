package kz.greetgo.kafka.producer;

import java.util.concurrent.ExecutionException;

public class RuntimeExecutionException extends RuntimeException {
  public RuntimeExecutionException(ExecutionException e) {
    super(e);
  }
}
