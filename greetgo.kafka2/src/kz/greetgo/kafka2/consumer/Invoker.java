package kz.greetgo.kafka2.consumer;

import kz.greetgo.kafka2.model.Box;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface Invoker {
  /**
   * @return needless to commit
   */
  boolean invoke(ConsumerRecords<byte[], Box> records);

  boolean isAutoCommit();

  String getConsumerName();
}
