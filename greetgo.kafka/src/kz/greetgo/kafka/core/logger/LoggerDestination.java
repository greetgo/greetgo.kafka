package kz.greetgo.kafka.core.logger;

import org.apache.kafka.common.errors.WakeupException;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.function.Supplier;

public interface LoggerDestination {

  void logProducerConfigOnCreating(String producerName, Map<String, Object> configMap);

  void logProducerClosed(String producerName);

  void logConsumerWakeupExceptionHappened(WakeupException wakeupException);

  void logConsumerStartWorker(String consumerInfo, long workerId);

  void logConsumerFinishWorker(String consumerInfo, long workerId);

  void logConsumerErrorInMethod(Throwable throwable, String consumerName, Object controller, Method method);

  void logConsumerWorkerConfig(String consumerInfo, long workerId, Map<String, Object> configMap);

  void logConsumerIllegalAccessExceptionInvokingMethod(IllegalAccessException e, String consumerName,
                                                       Object controller, Method method);

  void debug(Supplier<String> message);

}
