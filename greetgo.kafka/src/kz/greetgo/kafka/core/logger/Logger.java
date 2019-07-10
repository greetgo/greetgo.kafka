package kz.greetgo.kafka.core.logger;

import kz.greetgo.kafka.consumer.ConsumerDefinition;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Logger implements LoggerExternal {

  private LoggerDestination destination = null;

  @Override
  public void setDestination(LoggerDestination destination) {
    this.destination = destination;
  }

  @Override
  public void setDestination(LogMessageAcceptor acceptor) {
    if (acceptor == null) {
      destination = null;
    } else {
      destination = LoggerDestinationMessageBridge.of(acceptor);
    }
  }

  private final ConcurrentHashMap<LoggerType, Boolean> showings = new ConcurrentHashMap<>();

  @Override
  public void setShowLogger(LoggerType loggerType, boolean show) {
    showings.put(loggerType, show);
  }

  public boolean isShow(LoggerType loggerType) {
    if (destination == null) {
      return false;
    }
    {
      Boolean showing = showings.get(loggerType);
      return showing == null ? false : showing;
    }
  }

  public void logProducerConfigOnCreating(String producerName, Map<String, Object> configMap) {
    LoggerDestination d = this.destination;
    if (d != null) {
      d.logProducerConfigOnCreating(producerName, configMap);
    }
  }

  public void logProducerClosed(String producerName) {
    LoggerDestination d = this.destination;
    if (d != null) {
      d.logProducerClosed(producerName);
    }
  }

  public void logConsumerStartWorker(ConsumerDefinition consumerDefinition, long workerId) {
    LoggerDestination d = this.destination;
    if (d != null) {
      d.logConsumerStartWorker(consumerDefinition, workerId);
    }
  }

  public void logConsumerFinishWorker(ConsumerDefinition consumerDefinition, long workerId) {
    LoggerDestination d = this.destination;
    if (d != null) {
      d.logConsumerFinishWorker(consumerDefinition, workerId);
    }
  }

  public void logConsumerErrorInMethod(Throwable throwable,
                                       String consumerName,
                                       Object controller, Method method) {

    LoggerDestination d = this.destination;
    if (d != null) {
      d.logConsumerErrorInMethod(throwable, consumerName, controller, method);
    }

  }

  public void logConsumerWorkerConfig(ConsumerDefinition consumerDefinition,
                                      long workerId, Map<String, Object> configMap) {

    LoggerDestination d = this.destination;
    if (d != null) {
      d.logConsumerWorkerConfig(consumerDefinition, workerId, configMap);
    }

  }

  public void logConsumerIllegalAccessExceptionInvokingMethod(IllegalAccessException e,
                                                              String consumerName,
                                                              Object controller, Method method) {

    LoggerDestination d = this.destination;
    if (d != null) {
      d.logConsumerIllegalAccessExceptionInvokingMethod(e, consumerName, controller, method);
    }

  }

  public void logConsumerReactorRefresh(ConsumerDefinition consumerDefinition, int currentCount, int workerCount) {
    LoggerDestination d = this.destination;
    if (d != null) {
      d.logConsumerReactorRefresh(consumerDefinition, currentCount, workerCount);
    }
  }

  public void logConsumerPollExceptionHappened(RuntimeException exception, ConsumerDefinition consumerDefinition) {
    LoggerDestination d = this.destination;
    if (d != null) {
      d.logConsumerPollExceptionHappened(exception, consumerDefinition);
    }
  }

  public void logConsumerCommitSyncExceptionHappened(RuntimeException exception, ConsumerDefinition consumerDefinition) {
    LoggerDestination d = this.destination;
    if (d != null) {
      d.logConsumerCommitSyncExceptionHappened(exception, consumerDefinition);
    }
  }

}
