package kz.greetgo.kafka.producer;

import kz.greetgo.kafka.core.ProducerSynchronizer;
import kz.greetgo.kafka.core.logger.LoggerType;
import kz.greetgo.kafka.model.Box;
import kz.greetgo.kafka.serializer.BoxSerializer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ProducerFacadeBridge implements ProducerFacade {

  private final ProducerSource source;
  private final String producerName;
  private final boolean autoResettable;
  private final ProducerSynchronizer producerSynchronizer;

  private ProducerFacadeBridge(String producerName, ProducerSource source,
                               boolean autoResettable, ProducerSynchronizer producerSynchronizer) {
    this.source = source;
    this.producerName = producerName;
    this.autoResettable = autoResettable;
    this.producerSynchronizer = producerSynchronizer;
  }

  public static ProducerFacadeBridge createAutoResettableBridge(String producerName,
                                                                ProducerSource source,
                                                                ProducerSynchronizer producerSynchronizer) {

    return new ProducerFacadeBridge(producerName, source, true, producerSynchronizer);
  }

  public static ProducerFacadeBridge createPermanentBridge(String producerName,
                                                           ProducerSource source,
                                                           ProducerSynchronizer producerSynchronizer) {

    return new ProducerFacadeBridge(producerName, source, false, producerSynchronizer);
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

    if (autoResettable && creationTimestamp.get() < source.getProducerConfigUpdateTimestamp(producerName)) {
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

  @Override
  public KafkaSending sending(Object body) {
    final KafkaSendWorker sendWorker = new KafkaSendWorker(body, this::getNativeProducer, source);
    return new KafkaSending() {

      @Override
      public KafkaSending toTopic(String topic) {
        sendWorker.toTopic(topic);
        return this;
      }

      @Override
      public KafkaSending toPartition(int partition) {
        sendWorker.toPartition(partition);
        return this;
      }

      @Override
      public KafkaSending setTimestamp(Long timestamp) {
        sendWorker.setTimestamp(timestamp);
        return this;
      }

      @Override
      public KafkaSending addHeader(String key, byte[] value) {
        sendWorker.addHeader(key, value);
        return this;
      }

      @Override
      public KafkaSending addConsumerToIgnore(String consumerName) {
        sendWorker.addConsumerToIgnore(consumerName);
        return this;
      }

      @Override
      public KafkaSending setAuthor(String author) {
        sendWorker.setAuthor(author);
        return this;
      }

      @Override
      public KafkaSending withKey(String keyAsString) {
        sendWorker.withKey(keyAsString);
        return this;
      }

      @Override
      public KafkaSending withKey(byte[] keyAsBytes) {
        sendWorker.withKey(keyAsBytes);
        return this;
      }

      @Override
      public KafkaFuture go() {
        return sendWorker.go();
      }

    };
  }

  @Override
  public KafkaPortionSending portionSending(Object body) {
    final KafkaSendWorker sendWorker = new KafkaSendWorker(body, this::getNativeProducer, source);
    return new KafkaPortionSending() {

      @Override
      public KafkaPortionSending toTopic(String topic) {
        sendWorker.toTopic(topic);
        return this;
      }

      @Override
      public KafkaPortionSending toPartition(int partition) {
        sendWorker.toPartition(partition);
        return this;
      }

      @Override
      public KafkaPortionSending setTimestamp(Long timestamp) {
        sendWorker.setTimestamp(timestamp);
        return this;
      }

      @Override
      public KafkaPortionSending addHeader(String key, byte[] value) {
        sendWorker.addHeader(key, value);
        return this;
      }

      @Override
      public KafkaPortionSending addConsumerToIgnore(String consumerName) {
        sendWorker.addConsumerToIgnore(consumerName);
        return this;
      }

      @Override
      public KafkaPortionSending setAuthor(String author) {
        sendWorker.setAuthor(author);
        return this;
      }

      @Override
      public KafkaPortionSending withKey(String keyAsString) {
        sendWorker.withKey(keyAsString);
        return this;
      }

      @Override
      public KafkaPortionSending withKey(byte[] keyAsBytes) {
        sendWorker.withKey(keyAsBytes);
        return this;
      }

      @Override
      public void go() {
        producerSynchronizer.acceptKafkaFuture(sendWorker.go());
      }

    };
  }
}
