package kz.greetgo.kafka.core.logger;

import org.apache.kafka.common.errors.WakeupException;

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

  public void logConsumerWakeupExceptionHappened(WakeupException wakeupException) {
    LoggerDestination d = this.destination;
    if (d != null) {
      d.logConsumerWakeupExceptionHappened(wakeupException);
    }
  }

  public void logConsumerStartWorker(String consumerInfo, long workerId) {
    LoggerDestination d = this.destination;
    if (d != null) {
      d.logConsumerStartWorker(consumerInfo, workerId);
    }
  }

  public void logConsumerFinishWorker(String consumerInfo, long workerId) {
    LoggerDestination d = this.destination;
    if (d != null) {
      d.logConsumerFinishWorker(consumerInfo, workerId);
    }
  }

  public void logConsumerErrorInMethod(Throwable throwable, String consumerName, Object controller, Method method) {
    LoggerDestination d = this.destination;
    if (d != null) {
      d.logConsumerErrorInMethod(throwable, consumerName, controller, method);
    }
  }

  public void logConsumerWorkerConfig(String consumerInfo, long workerId, Map<String, Object> configMap) {
    LoggerDestination d = this.destination;
    if (d != null) {
      d.logConsumerWorkerConfig(consumerInfo, workerId, configMap);
    }
  }

  public void logConsumerIllegalAccessExceptionInvokingMethod(IllegalAccessException e, String consumerName,
                                                              Object controller, Method method) {
    LoggerDestination d = this.destination;
    if (d != null) {
      d.logConsumerIllegalAccessExceptionInvokingMethod(e, consumerName, controller, method);
    }
  }
}
