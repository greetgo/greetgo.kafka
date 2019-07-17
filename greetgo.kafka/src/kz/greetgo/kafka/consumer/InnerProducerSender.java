package kz.greetgo.kafka.consumer;

public interface InnerProducerSender {

  interface Sending {
    Sending toTopic(String topic);

    Sending toPartition(int partition);

    Sending setTimestamp(Long timestamp);

    Sending addConsumerToIgnore(String consumerName);

    Sending setAuthor(String author);

    Sending addHeader(String key, byte[] value);

    void go();
  }

  Sending sending(Object model);

}
