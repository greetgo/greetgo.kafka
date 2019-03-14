package kz.greetgo.kafka2.producer;

public interface KafkaSending {

  KafkaSending toTopic(String topic);

  KafkaSending toPartition(int partition);

  KafkaFuture go();

}
