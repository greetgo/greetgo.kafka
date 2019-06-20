package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.core.logger.LoggerDestination;
import org.apache.kafka.common.errors.WakeupException;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.function.Supplier;

public class TestLoggerDestinationInteractive implements LoggerDestination {

  @Override
  public void logConsumerErrorInMethod(Throwable throwable, String consumerName, Object controller, Method method) {
    System.out.println("********************* logConsumerErrorInMethod");
  }

  @Override
  public void logProducerConfigOnCreating(String producerName, Map<String, Object> configMap) {
    System.out.println("********************* logProducerConfigOnCreating");
  }

  @Override
  public void logProducerClosed(String producerName) {
    System.out.println("********************* logProducerClosed");
  }

  @Override
  public void logConsumerWakeupExceptionHappened(WakeupException wakeupException) {
    System.out.println("********************* logConsumerWakeupExceptionHappened");
  }

  @Override
  public void logConsumerStartWorker(String consumerInfo, long workerId) {
    System.out.println("********************* logConsumerStartWorker");
  }

  @Override
  public void logConsumerFinishWorker(String consumerInfo, long workerId) {
    System.out.println("********************* logConsumerFinishWorker");
  }

  @Override
  public void logConsumerWorkerConfig(String consumerInfo, long workerId, Map<String, Object> configMap) {
    System.out.println("********************* logConsumerWorkerConfig");
  }

  @Override
  public void logConsumerIllegalAccessExceptionInvokingMethod(IllegalAccessException e,
                                                              String consumerName,
                                                              Object controller,
                                                              Method method) {

    System.out.println("********************* logConsumerIllegalAccessExceptionInvokingMethod");

  }

  @Override
  public void debug(Supplier<String> message) {
    System.out.println("********************* logInfo " + message.get());
  }

}
