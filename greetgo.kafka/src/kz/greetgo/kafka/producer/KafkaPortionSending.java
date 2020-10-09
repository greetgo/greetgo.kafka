package kz.greetgo.kafka.producer;

public interface KafkaPortionSending {

  KafkaPortionSending toTopic(String topic);

  KafkaPortionSending toPartition(int partition);

  KafkaPortionSending setTimestamp(Long timestamp);

  KafkaPortionSending addConsumerToIgnore(String consumerName);

  KafkaPortionSending setAuthor(String author);

  KafkaPortionSending addHeader(String key, byte[] value);

  KafkaPortionSending withKey(String keyAsString);

  KafkaPortionSending withKey(byte[] keyAsBytes);

  void go();

}
