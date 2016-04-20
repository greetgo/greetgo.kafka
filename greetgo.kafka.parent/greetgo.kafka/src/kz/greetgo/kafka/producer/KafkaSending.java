package kz.greetgo.kafka.producer;

public interface KafkaSending extends AutoCloseable {
  void send(Object object);
}
