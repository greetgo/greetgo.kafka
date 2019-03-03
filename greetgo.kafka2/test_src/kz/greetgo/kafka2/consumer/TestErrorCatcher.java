package kz.greetgo.kafka2.consumer;

import java.util.ArrayList;
import java.util.List;

public class TestErrorCatcher implements ErrorCatcher {

  public final List<Throwable> errorList = new ArrayList<>();

  @Override
  public void catchError(Throwable throwable) {
    errorList.add(throwable);
  }
}
