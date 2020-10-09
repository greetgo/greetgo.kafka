package kz.greetgo.kafka.core;

public interface ConsumerPortionInvoking extends AutoCloseable {
  @Override
  void close();
}
