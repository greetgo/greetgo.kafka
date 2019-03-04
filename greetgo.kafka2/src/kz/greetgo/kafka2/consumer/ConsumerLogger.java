package kz.greetgo.kafka2.consumer;

import org.apache.kafka.common.errors.WakeupException;

import java.util.Map;

public interface ConsumerLogger {
  void wakeupExceptionHappened(WakeupException wakeupException);

  void startWorker(String consumerInfo, long workerId);

  void finishWorker(String consumerInfo, long workerId);

  void errorInMethod(Throwable throwable);

  void showWorkerConfig(String consumerInfo, long workerId, Map<String, Object> configMap);
}
