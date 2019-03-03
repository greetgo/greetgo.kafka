package kz.greetgo.kafka2.util;

import kz.greetgo.kafka2.consumer.ConsumerLogger;
import org.apache.kafka.common.errors.WakeupException;

public class EmptyConsumerLogger implements ConsumerLogger {
  @Override
  public void wakeupExceptionHappened(WakeupException wakeupException) {}

  @Override
  public void startWorker(String consumerInfo, long workerId) {}

  @Override
  public void finishWorker(String consumerInfo, long workerId) {}

  @Override
  public void errorInMethod(Throwable throwable) {}
}
