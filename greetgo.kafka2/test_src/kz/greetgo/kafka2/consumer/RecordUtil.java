package kz.greetgo.kafka2.consumer;

import kz.greetgo.kafka2.model.Box;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;

import java.util.Date;
import java.util.List;

import static java.util.stream.Collectors.groupingBy;

public class RecordUtil {
  public static ConsumerRecord<byte[], Box> recordOf(String topic, byte[] key, Box box) {
    return new ConsumerRecord<>(topic, 1, 1, key, box);
  }

  public static ConsumerRecords<byte[], Box> recordsOf(List<ConsumerRecord<byte[], Box>> list) {
    return new ConsumerRecords<>(
      list.stream()
        .collect(groupingBy(rec -> new TopicPartition(rec.topic(), 1)))
    );
  }

  public static ConsumerRecord<byte[], Box> recordWithTimestamp(String topic, Date timestamp, Box box) {
    return new ConsumerRecord<>(topic, 1, 1, timestamp.getTime(), TimestampType.CREATE_TIME, 0, 1, 1, new byte[0], box);
  }

  public static ConsumerRecord<byte[], Box> recordWithPartition(String topic, int partition, Box box) {
    return new ConsumerRecord<>(topic, partition, 1, new byte[0], box);
  }

  public static ConsumerRecord<byte[], Box> recordWithOffset(String topic, long offset, Box box) {
    return new ConsumerRecord<>(topic, 1, offset, new byte[0], box);
  }
}
