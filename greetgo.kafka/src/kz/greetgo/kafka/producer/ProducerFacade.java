package kz.greetgo.kafka.producer;

import kz.greetgo.kafka.model.Box;
import org.apache.kafka.clients.producer.Producer;

import java.util.Map;

public interface ProducerFacade {

  void reset();

  Producer<byte[], Box> getNativeProducer();

  Map<String, Object> getConfigData();

  KafkaSending sending(Object body);

}
