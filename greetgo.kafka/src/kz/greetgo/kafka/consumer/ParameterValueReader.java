package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.model.Box;
import kz.greetgo.kafka.producer.KafkaFuture;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public interface ParameterValueReader {
  Object read(ConsumerRecord<byte[], Box> record, InvokeSessionContext invokeSessionContext);

  default Set<String> getProducerNames() {
    return Collections.emptySet();
  }

  default Class<?> gettingBodyClass() {
    return null;
  }

  default List<KafkaFuture> getKafkaFutures() {
    return Collections.emptyList();
  }
}
