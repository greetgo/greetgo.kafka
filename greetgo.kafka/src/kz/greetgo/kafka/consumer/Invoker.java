package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.model.Box;
import kz.greetgo.kafka.producer.ProducerFacade;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Set;

public interface Invoker {

  Set<String> getUsingProducerNames();

  interface InvokeResult {
    boolean needToCommit();

    Throwable exceptionInMethod();
  }

  interface InvokeSession extends AutoCloseable {

    void putProducer(String producerName, ProducerFacade producer);

    InvokeResult invoke(ConsumerRecords<byte[], Box> records);

    @Override
    void close();
  }

  InvokeSession createSession();

  boolean isAutoCommit();

  String getConsumerName();
}
