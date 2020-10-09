package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.core.logger.LoggerDestination;

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
  public void logConsumerStartWorker(ConsumerDefinition consumerDefinition, long workerId) {
    System.out.println("********************* logConsumerStartWorker");
  }

  @Override
  public void logConsumerFinishWorker(ConsumerDefinition consumerDefinition, long workerId) {
    System.out.println("********************* logConsumerFinishWorker");
  }

  @Override
  public void logConsumerWorkerConfig(ConsumerDefinition consumerDefinition, long workerId, Map<String, Object> configMap) {
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

  @Override
  public void logConsumerReactorRefresh(ConsumerDefinition consumerDefinition, int currentCount, int workerCount) {
    System.out.println("********************* logConsumerReactorRefresh");
  }

  @Override
  public void logConsumerPollExceptionHappened(RuntimeException exception, ConsumerDefinition consumerDefinition) {
    System.out.println("********************* logConsumerPollExceptionHappened");
  }

  @Override
  public void logConsumerCommitSyncExceptionHappened(RuntimeException exception, ConsumerDefinition consumerDefinition) {
    System.out.println("********************* logConsumerCommitSyncExceptionHappened");
  }

  @Override
  public void logProducerCreated(String producerName) {
    System.out.println("********************* logProducerCreated");
  }

  @Override
  public void logProducerValidationError(Throwable error) {
    System.out.println("********************* logProducerValidationError");
  }

  @Override
  public void logProducerAwaitAndGetError(String errorCode, Exception exception) {
    System.out.println("********************* logProducerAwaitAndGetError");
  }

}
