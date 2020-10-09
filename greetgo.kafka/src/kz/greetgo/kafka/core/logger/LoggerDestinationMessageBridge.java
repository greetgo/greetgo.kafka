package kz.greetgo.kafka.core.logger;

import kz.greetgo.kafka.consumer.ConsumerDefinition;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.function.Supplier;

public class LoggerDestinationMessageBridge implements LoggerDestination {

  private final LogMessageAcceptor acceptor;

  private LoggerDestinationMessageBridge(LogMessageAcceptor acceptor) {
    this.acceptor = acceptor;
  }

  public static LoggerDestinationMessageBridge of(LogMessageAcceptor acceptor) {
    return new LoggerDestinationMessageBridge(acceptor);
  }

  @Override
  public void logProducerConfigOnCreating(String producerName, Map<String, Object> configMap) {
    if (acceptor.isInfoEnabled()) {
      StringBuilder sb = new StringBuilder();
      sb.append("Created producer `").append(producerName).append("` with config:");
      configMap
        .entrySet()
        .stream()
        .sorted(Map.Entry.comparingByKey())
        .forEachOrdered(e ->
          sb.append("\n    ").append(e.getKey()).append(" = `").append(e.getValue()).append("`")
        );
      acceptor.info(sb.toString());
    }
  }

  @Override
  public void logProducerClosed(String producerName) {
    if (acceptor.isInfoEnabled()) {
      acceptor.info("Closed producer `" + producerName + "`");
    }
  }

  @Override
  public void logProducerCreated(String producerName) {
    if (acceptor.isInfoEnabled()) {
      acceptor.info("Created producer `" + producerName + "`");
    }
  }

  @Override
  public void logConsumerStartWorker(ConsumerDefinition consumerDefinition, long workerId) {
    if (acceptor.isInfoEnabled()) {
      acceptor.info("Started consumer worker `" + consumerDefinition.logDisplay()
        + "` with id = " + workerId + " in thread " + Thread.currentThread().getName());
    }
  }

  @Override
  public void logConsumerFinishWorker(ConsumerDefinition consumerDefinition, long workerId) {
    if (acceptor.isInfoEnabled()) {
      acceptor.info("Finished consumer worker `" + consumerDefinition.logDisplay() + "` with id = " + workerId);
    }
  }

  @Override
  public void debug(Supplier<String> message) {
    if (acceptor.isDebugEnabled()) {
      acceptor.debug(message.get());
    }
  }

  @Override
  public void logConsumerErrorInMethod(Throwable throwable, String consumerName, Object controller, Method method) {
    //noinspection StringBufferReplaceableByString
    StringBuilder sb = new StringBuilder();
    sb.append("Error in consumer `")
      .append(consumerName)
      .append("` of ")
      .append(controller.getClass().getName())
      .append("#")
      .append(method.getName())
      .append("() ");

    acceptor.error(sb.toString(), throwable);
  }

  @Override
  public void logConsumerWorkerConfig(ConsumerDefinition consumerDefinition, long workerId, Map<String, Object> configMap) {

    if (!acceptor.isInfoEnabled()) {
      return;
    }

    StringBuilder sb = new StringBuilder();

    sb.append("Consumer worker config: consumer = `").append(consumerDefinition.logDisplay())
      .append("`, workerId = ").append(workerId).append("\n");

    configMap
      .entrySet()
      .stream()
      .sorted(Map.Entry.comparingByKey())
      .forEachOrdered(e ->
        sb.append("\n    ").append(e.getKey()).append(" = `").append(e.getValue()).append("`")
      );

    acceptor.info(sb.toString());

  }

  @Override
  public void logConsumerIllegalAccessExceptionInvokingMethod(IllegalAccessException e, String consumerName,
                                                              Object controller, Method method) {

    //noinspection StringBufferReplaceableByString
    StringBuilder sb = new StringBuilder();
    sb.append("IllegalAccessException invoking method in consumer `")
      .append(consumerName)
      .append("` in ")
      .append(controller.getClass().getName())
      .append("#")
      .append(method.getName())
      .append("()");

    acceptor.error(sb.toString(), e);
  }

  @Override
  public void logConsumerPollExceptionHappened(RuntimeException exception, ConsumerDefinition consumerDefinition) {

    acceptor.error("Poll exception in consumer " + consumerDefinition.logDisplay(), exception);

  }

  @Override
  public void logConsumerCommitSyncExceptionHappened(RuntimeException exception, ConsumerDefinition consumerDefinition) {

    acceptor.error("CommitSync exception in consumer " + consumerDefinition.logDisplay(), exception);

  }

  @Override
  public void logProducerValidationError(Throwable error) {
    acceptor.error("Producer validation error", error);
  }

  @Override
  public void logProducerAwaitAndGetError(String errorCode, Exception exception) {
    acceptor.error(errorCode + " :: Producer AwaitAndGet Error", exception);
  }

  @Override
  public void logConsumerReactorRefresh(ConsumerDefinition consumerDefinition, int currentCount, int workerCount) {
    if (!acceptor.isInfoEnabled()) {
      return;
    }

    acceptor.info("Refresh consumer count = "
      + currentCount + " --> " + workerCount + "; " + consumerDefinition.logDisplay());
  }
}
