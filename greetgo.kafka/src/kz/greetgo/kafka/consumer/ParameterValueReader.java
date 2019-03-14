package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.model.Box;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ParameterValueReader {
  Object read(ConsumerRecord<byte[], Box> record);

  default Class<?> gettingBodyClass() {
    return null;
  }
}
