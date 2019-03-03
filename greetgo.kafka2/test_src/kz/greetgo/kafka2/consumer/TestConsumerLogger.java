package kz.greetgo.kafka2.consumer;

import kz.greetgo.kafka2.util.EmptyConsumerLogger;

import java.util.ArrayList;
import java.util.List;

public class TestConsumerLogger extends EmptyConsumerLogger {

  public final List<Throwable> errorList = new ArrayList<>();

  @Override
  public void errorInMethod(Throwable throwable) {
    errorList.add(throwable);
  }

}
