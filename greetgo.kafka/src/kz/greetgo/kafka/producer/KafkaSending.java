package kz.greetgo.kafka.producer;

public interface KafkaSending {

  KafkaSending toTopic(String topic);

  KafkaSending toPartition(int partition);

  KafkaSending setTimestamp(Long timestamp);

  KafkaSending addConsumerToIgnore(String consumerName);

  KafkaSending setAuthor(String author);

  KafkaSending addHeader(String key, byte[] value);

  KafkaSending withKey(String keyAsString);

  KafkaSending withKey(byte[] keyAsBytes);

  KafkaSending kafkaId(String kafkaId);

  KafkaFuture go();

}
