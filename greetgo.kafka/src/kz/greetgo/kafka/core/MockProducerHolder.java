package kz.greetgo.kafka.core;

import kz.greetgo.kafka.model.Box;
import kz.greetgo.kafka.serializer.BoxSerializer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.List;

public class MockProducerHolder {
  private final String producerName;
  private final ByteArraySerializer keySerializer;
  private final BoxSerializer valueSerializer;

  private final Cluster cluster;
  private final Partitioner partitioner;
  private final MockProducer<byte[], Box> producer;

  public MockProducerHolder(final String producerName,
                            final ByteArraySerializer keySerializer,
                            final BoxSerializer valueSerializer) {
    this.producerName = producerName;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;

    cluster = Cluster.empty();
    partitioner = new DefaultPartitioner();

    producer = new MockProducer<>(cluster, true, partitioner, keySerializer, valueSerializer);
  }

  public String getProducerName() {
    return producerName;
  }

  public MockProducer<byte[], Box> getProducer() {
    return producer;
  }

  public Partitioner getPartitioner() {
    return partitioner;
  }

  public Cluster getCluster() {
    return cluster;
  }

  public TopicPartition topicPartition(ProducerRecord<byte[], Box> record) {
    int partition = 0;
    if (!this.cluster.partitionsForTopic(record.topic()).isEmpty()) {
      partition = partition(record, this.cluster);
    }
    return new TopicPartition(record.topic(), partition);
  }

  private int partition(ProducerRecord<byte[], Box> record, Cluster cluster) {
    Integer partition = record.partition();
    String topic = record.topic();
    if (partition != null) {
      List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
      int numPartitions = partitions.size();
      // they have given us a partition, use it
      if (partition < 0 || partition >= numPartitions)
        throw new IllegalArgumentException("Invalid partition given with record: " + partition
          + " is not in the range [0..."
          + numPartitions
          + "].");
      return partition;
    }
    byte[] keyBytes = keySerializer.serialize(topic, record.headers(), record.key());
    byte[] valueBytes = valueSerializer.serialize(topic, record.headers(), record.value());
    return this.partitioner.partition(topic, record.key(), keyBytes, record.value(), valueBytes, cluster);
  }
}
