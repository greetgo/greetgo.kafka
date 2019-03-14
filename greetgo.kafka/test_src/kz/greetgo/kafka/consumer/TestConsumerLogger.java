package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.util.EmptyConsumerLogger;

import java.util.ArrayList;
import java.util.List;

public class TestConsumerLogger extends EmptyConsumerLogger {

  public final List<Throwable> errorList = new ArrayList<>();

  @Override
  public void errorInMethod(Throwable throwable) {
    errorList.add(throwable);
  }

}
