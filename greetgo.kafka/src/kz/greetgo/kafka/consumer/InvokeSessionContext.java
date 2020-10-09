package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.producer.KafkaFuture;

import java.util.ArrayList;
import java.util.List;

public class InvokeSessionContext {
  public final List<KafkaFuture> kafkaFutures = new ArrayList<>();

  public void close() {}
}
