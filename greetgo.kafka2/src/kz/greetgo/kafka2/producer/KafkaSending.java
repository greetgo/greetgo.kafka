package kz.greetgo.kafka2.producer;

import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

public interface KafkaSending {

  KafkaSending toTopic(String topic);

  KafkaSending toPartition(int partition);

  Future<RecordMetadata> go();

}
