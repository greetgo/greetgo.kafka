package kz.greetgo.kafka.core.logger;

import kz.greetgo.kafka.consumer.ConsumerDefinition;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.function.Supplier;

public interface LoggerDestination {

  void logProducerConfigOnCreating(String producerName, Map<String, Object> configMap);

  void logProducerClosed(String producerName);

  void logConsumerStartWorker(ConsumerDefinition consumerDefinition, long workerId);

  void logConsumerFinishWorker(ConsumerDefinition consumerDefinition, long workerId);

  void logConsumerErrorInMethod(Throwable throwable, String consumerName,
                                Object controller, Method method);

  void logConsumerWorkerConfig(ConsumerDefinition consumerDefinition, long workerId, Map<String, Object> configMap);

  void logConsumerIllegalAccessExceptionInvokingMethod(IllegalAccessException e, String consumerName,
                                                       Object controller, Method method);

  void debug(Supplier<String> message);

  void logConsumerReactorRefresh(ConsumerDefinition consumerDefinition, int currentCount, int workerCount);

  void logConsumerPollExceptionHappened(RuntimeException exception, ConsumerDefinition consumerDefinition);

  void logConsumerCommitSyncExceptionHappened(RuntimeException exception, ConsumerDefinition consumerDefinition);

  void logProducerCreated(String producerName);

  void logProducerValidationError(Throwable error);

  void logProducerAwaitAndGetError(String errorCode, Exception exception);
}
