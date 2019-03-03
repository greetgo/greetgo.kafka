package kz.greetgo.kafka2.consumer;

import org.apache.kafka.common.errors.WakeupException;

public interface ConsumerLogger {
  void wakeupExceptionHappened(WakeupException wakeupException);

  void startWorker(String consumerInfo, long workerId);

  void finishWorker(String consumerInfo, long workerId);
}
