package kz.greetgo.kafka.core.logger;

import kz.greetgo.kafka.consumer.ConsumerDefinition;
import org.apache.kafka.common.errors.WakeupException;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Comparator.comparing;

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
        .sorted(comparing(Map.Entry::getKey))
        .forEachOrdered(e ->
          sb.append("\n    ").append(e.getKey()).append(" = `").append(e.getValue()).append("`")
        );
      acceptor.info(sb.toString());
    }
  }

  @Override
  public void logProducerClosed(String producerName) {
    acceptor.info("Closed producer `" + producerName + "`");
  }

  private static void appendStackTrace(StringBuilder sb, Throwable throwable) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      PrintStream printStream = new PrintStream(outputStream, false, "UTF-8");
      throwable.printStackTrace(printStream);
      printStream.flush();

      sb.append(outputStream.toString("UTF-8"));

    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void logConsumerWakeupExceptionHappened(WakeupException wakeupException) {
    StringBuilder sb = new StringBuilder();
    sb.append("Wakeup exception happened:\n");
    appendStackTrace(sb, wakeupException);
    acceptor.error(sb.toString());
  }

  @Override
  public void logConsumerStartWorker(String consumerInfo, long workerId) {
    if (acceptor.isInfoEnabled()) {
      acceptor.info("Started consumer worker `" + consumerInfo
        + "` with id = " + workerId + " in thread " + Thread.currentThread().getName());
    }
  }

  @Override
  public void logConsumerFinishWorker(String consumerInfo, long workerId) {
    if (acceptor.isInfoEnabled()) {
      acceptor.info("Finished consumer worker `" + consumerInfo + "` with id = " + workerId);
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
    StringBuilder sb = new StringBuilder();
    sb.append("Error in consumer `")
      .append(consumerName)
      .append("` of ")
      .append(controller.getClass().getName())
      .append("#")
      .append(method.getName())
      .append("() ")
      .append(":\n");

    appendStackTrace(sb, throwable);

    acceptor.error(sb.toString());
  }

  @Override
  public void logConsumerWorkerConfig(String consumerInfo, long workerId, Map<String, Object> configMap) {

    if (!acceptor.isInfoEnabled()) {
      return;
    }

    StringBuilder sb = new StringBuilder();

    sb.append("Consumer worker config: consumer = `").append(consumerInfo)
      .append("`, workerId = ").append(workerId).append("\n");

    configMap
      .entrySet()
      .stream()
      .sorted(comparing(Map.Entry::getKey))
      .forEachOrdered(e ->
        sb.append("\n    ").append(e.getKey()).append(" = `").append(e.getValue()).append("`")
      );

    acceptor.info(sb.toString());

  }

  @Override
  public void logConsumerIllegalAccessExceptionInvokingMethod(IllegalAccessException e, String consumerName, Object controller, Method method) {
    StringBuilder sb = new StringBuilder();
    sb.append("IllegalAccessException invoking method in consumer `")
      .append(consumerName)
      .append("` in ")
      .append(controller.getClass().getName())
      .append("#")
      .append(method.getName())
      .append("() :\n");

    appendStackTrace(sb, e);

    acceptor.error(sb.toString());
  }

  @Override
  public void logConsumerReactorRefresh(ConsumerDefinition consumerDefinition, int currentCount, int workerCount) {
    if (!acceptor.isInfoEnabled()) {
      return;
    }

    acceptor.info("Refresh consumer : currentCount = "
      + currentCount + ", newCount = " + workerCount + "; " + consumerDefinition.logDisplay());
  }
}
