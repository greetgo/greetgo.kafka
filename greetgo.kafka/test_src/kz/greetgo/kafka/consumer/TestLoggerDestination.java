package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.core.logger.LoggerDestination;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class TestLoggerDestination implements LoggerDestination {

  public final List<Throwable> errorList = new ArrayList<>();

  @Override
  public void logConsumerErrorInMethod(Throwable throwable, String consumerName, Object controller, Method method) {
    errorList.add(throwable);
  }

  @Override
  public void logProducerConfigOnCreating(String producerName, Map<String, Object> configMap) {}

  @Override
  public void logProducerClosed(String producerName) {}

  @Override
  public void logConsumerIllegalAccessExceptionInvokingMethod
    (IllegalAccessException e, String consumerName, Object controller, Method method) {}

  @Override
  public void debug(Supplier<String> message) {}

  @Override
  public void logConsumerReactorRefresh(ConsumerDefinition consumerDefinition, int currentCount, int workerCount) {}

  @Override
  public void logConsumerStartWorker(ConsumerDefinition consumerDefinition, long workerId) {}

  @Override
  public void logConsumerFinishWorker(ConsumerDefinition consumerDefinition, long workerId) {}

  @Override
  public void logConsumerWorkerConfig
    (ConsumerDefinition consumerDefinition, long workerId, Map<String, Object> configMap) {}

  @Override
  public void logConsumerPollExceptionHappened(RuntimeException exception, ConsumerDefinition consumerDefinition) {}

  @Override
  public void logConsumerCommitSyncExceptionHappened
    (RuntimeException exception, ConsumerDefinition consumerDefinition) {}

  @Override
  public void logProducerCreated(String producerName) {}

  @Override
  public void logProducerValidationError(Throwable error) {}

  @Override
  public void logProducerAwaitAndGetError(String errorCode, Exception exception) {}
}
