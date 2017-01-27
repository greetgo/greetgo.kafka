package kz.greetgo.kafka.producer;

public interface KafkaSending extends AutoCloseable {
  void send(Object object);

  void sendDirect(String key, String value);

  @Override
  void close();
}
