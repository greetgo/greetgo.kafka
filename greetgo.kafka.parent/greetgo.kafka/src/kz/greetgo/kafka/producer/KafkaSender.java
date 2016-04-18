package kz.greetgo.kafka.producer;

public interface KafkaSender extends AutoCloseable {
  void send(Object object);
}
