package kz.greetgo.kafka.producer;

public interface KafkaSending {

  KafkaSending toTopic(String topic);

  KafkaSending toPartition(int partition);

  KafkaFuture go();

}
