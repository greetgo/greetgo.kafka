package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.producer.ProducerFacade;

import java.util.HashMap;
import java.util.Map;

public class InvokeSessionContext {
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