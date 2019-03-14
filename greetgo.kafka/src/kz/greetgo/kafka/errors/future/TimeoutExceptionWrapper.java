package kz.greetgo.kafka.errors.future;

import java.util.concurrent.TimeoutException;

public class TimeoutExceptionWrapper extends RuntimeException {
  public TimeoutExceptionWrapper(TimeoutException e) {
    super(e.getMessage(), e);
  }
}
