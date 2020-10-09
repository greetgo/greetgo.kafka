package kz.greetgo.kafka.producer;

import kz.greetgo.kafka.core.logger.LoggerType;
import kz.greetgo.kafka.model.Box;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toList;

public class KafkaSendWorker {
  private final Object body;
  private final Supplier<Producer<byte[], Box>> nativeProducerSupplier;
  private final ProducerSource source;

  private String topic = null;
  private Integer partition = null;
  private Long timestamp = null;
  private final ArrayList<Header> headers = new ArrayList<>();
  private final Set<String> ignorableConsumers = new HashSet<>();
  private String author;

  private byte[] withKey = null;

  public KafkaSendWorker(Object body, Supplier<Producer<byte[], Box>> nativeProducerSupplier,
                         ProducerSource producerSource) {
    this.body = body;
    this.nativeProducerSupplier = nativeProducerSupplier;
    this.source = producerSource;
  }

  public void toTopic(String topic) {
    this.topic = topic;
  }

  public void toPartition(Integer partition) {
    this.partition = partition;
  }

  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }

  public void addHeader(String key, byte[] value) {
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
  }

  public void addConsumerToIgnore(String consumerName) {
    ignorableConsumers.add(consumerName);
  }

  public void setAuthor(String author) {
    this.author = author;
  }

  public void withKey(String keyAsString) {
    withKey = keyAsString.getBytes(StandardCharsets.UTF_8);
  }

  public void withKey(byte[] keyAsBytes) {
    withKey = keyAsBytes;
  }

  public KafkaFuture go() {
    if (this.topic == null) {
      throw new RuntimeException("0Is7vLrG4Q :: topic == null");
    }

    Box box = new Box();
    box.body = body;
    box.a = author;
    box.i = ignorableConsumers.stream().sorted().collect(toList());

    try {
      box.validate();
    } catch (Throwable throwable) {

      if (source.logger().isShow(LoggerType.LOG_PRODUCER_VALIDATION_ERROR)) {
        source.logger().logProducerValidationError(throwable);
      }

      if (throwable instanceof RuntimeException) {
        throw (RuntimeException) throwable;
      }
      throw new RuntimeException(throwable);

    }

    byte[] key = withKey != null ? withKey : source.extractKey(body);

    return new KafkaFuture(
      nativeProducerSupplier.get().send(
        new ProducerRecord<>(topic, partition, timestamp, key, box, headers)
      )
    );

  }
}
