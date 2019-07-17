package kz.greetgo.kafka.producer;

import kz.greetgo.kafka.core.logger.LoggerType;
import kz.greetgo.kafka.model.Box;
import kz.greetgo.kafka.serializer.BoxSerializer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class ProducerFacade {

  private final ProducerSource source;
  private final String producerName;

  public ProducerFacade(String producerName, ProducerSource source) {
    this.source = source;
    this.producerName = producerName;
  }

  private final AtomicReference<Producer<byte[], Box>> producer = new AtomicReference<>(null);

  public void reset() {
    Producer<byte[], Box> producer = this.producer.getAndSet(null);
    if (producer != null) {

      producer.close();

      if (source.logger().isShow(LoggerType.LOG_CLOSE_PRODUCER)) {
        source.logger().logProducerClosed(producerName);
      }

    }
  }

  private final AtomicLong creationTimestamp = new AtomicLong(0);

  public Producer<byte[], Box> getNativeProducer() {

    if (creationTimestamp.get() < source.getProducerConfigUpdateTimestamp(producerName)) {
      reset();
    }

    {
      Producer<byte[], Box> ret = producer.get();
      if (ret != null) {
        return ret;
      }
    }

    return producer.updateAndGet(current -> current != null ? current : createProducer());

  }

  public Map<String, Object> getConfigData() {
    return source.getConfigFor(producerName);
  }

  private Producer<byte[], Box> createProducer() {
    ByteArraySerializer keySerializer = new ByteArraySerializer();
    BoxSerializer valueSerializer = new BoxSerializer(source.getStrConverter());
    Producer<byte[], Box> ret = source.createProducer(producerName, keySerializer, valueSerializer);
    creationTimestamp.set(source.getProducerConfigUpdateTimestamp(producerName));

    if (source.logger().isShow(LoggerType.LOG_CREATE_PRODUCER)) {
      source.logger().logProducerCreated(producerName);
    }

    return ret;
  }

  public KafkaSending sending(Object body) {
    return new KafkaSending() {

      String topic = null;

      @Override
      public KafkaSending toTopic(String topic) {
        this.topic = topic;
        return this;
      }

      Integer partition = null;

      @Override
      public KafkaSending toPartition(int partition) {
        this.partition = partition;
        return this;
      }

      Long timestamp = null;

      @SuppressWarnings("unused")
      public KafkaSending setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
        return this;
      }

      final ArrayList<Header> headers = new ArrayList<>();

      @SuppressWarnings("unused")
      public KafkaSending addHeader(String key, byte[] value) {
        headers.add(new Header() {
          @Override
          public String key() {
            return key;
          }

          @Override
          public byte[] value() {
            return value;
          }
        });
        return this;
      }

      final Set<String> ignorableConsumers = new HashSet<>();

      @SuppressWarnings("unused")
      public KafkaSending addConsumerToIgnore(String consumerName) {
        ignorableConsumers.add(consumerName);
        return this;
      }

      @Override
      public KafkaFuture go() {
        if (this.topic == null) {
          throw new RuntimeException("topic == null");
        }

        Box box = new Box();
        box.body = body;
        box.author = source.author();
        box.ignorableConsumers = ignorableConsumers.stream().sorted().collect(Collectors.toList());

        byte[] key = source.extractKey(body);

        return new KafkaFuture(
          getNativeProducer().send(
            new ProducerRecord<>(topic, partition, timestamp, key, box, headers)
          )
        );

      }

    };

  }

}
