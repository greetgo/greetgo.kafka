package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.model.Box;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface Invoker {

  interface InvokeResult {
    boolean needToCommit();

    Throwable exceptionInMethod();
  }

  interface InvokeSession extends AutoCloseable {

    InvokeResult invoke(ConsumerRecords<byte[], Box> records);

    @Override
    void close();
  }

  InvokeSession createSession();

  boolean isAutoCommit();

  String getConsumerName();
}
