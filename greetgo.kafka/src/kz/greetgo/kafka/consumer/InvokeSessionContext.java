package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.producer.KafkaFuture;
import kz.greetgo.kafka.producer.ProducerFacade;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InvokeSessionContext {
  public final List<KafkaFuture> kafkaFutures = new ArrayList<>();

  private final Map<String, ProducerFacade> producerMap = new HashMap<>();

  public void putProducer(String producerName, ProducerFacade producer) {
    producerMap.put(producerName, producer);
  }

  public void close() {
    producerMap.values().forEach(ProducerFacade::reset);
  }

  public ProducerFacade getProducer(String producerName) {
    return producerMap.get(producerName);
  }

}
