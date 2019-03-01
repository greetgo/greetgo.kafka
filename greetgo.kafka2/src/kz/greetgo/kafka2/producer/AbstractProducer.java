package kz.greetgo.kafka2.producer;

import kz.greetgo.kafka2.model.Box;
import kz.greetgo.kafka2.serializer.BoxSerializing;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class AbstractProducer {

  private final ProducerSource source;

  public AbstractProducer(ProducerSource source) {
    this.source = source;
  }

  private final AtomicReference<Producer<String, Box>> producer = new AtomicReference<>(null);

  public void reset() {
    Producer<String, Box> producer = this.producer.getAndSet(null);
    if (producer != null) {
      producer.close();
    }
  }

  private Producer<String, Box> getProducer() {
    {
      Producer<String, Box> ret = producer.get();
      if (ret != null) {
        return ret;
      }
    }
    return producer.updateAndGet(current -> current != null ? current : createProducer());
  }

  private Producer<String, Box> createProducer() {
    Map<String, Object> config = new HashMap<>();
    config.put("bootstrap.servers", source.bootstrapServers());
    config.put("acks", "all");
    config.put("delivery.timeout.ms", 35000);
    config.put("linger.ms", 1);
    config.put("request.timeout.ms", 30000);
    config.put("batch.size", 16384);
    config.put("buffer.memory", 33554432);

    source.performAdditionalProducerConfiguration(config);

    StringSerializer keySerializer = new StringSerializer();
    BoxSerializing valueSerializer = new BoxSerializing(source.getKryo());

    return new KafkaProducer<>(config, keySerializer, valueSerializer);
  }


  KafkaSending sending(Object body) {
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

      public KafkaSending setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
        return this;
      }

      final ArrayList<Header> headers = new ArrayList<>();

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

      public KafkaSending addConsumerToIgnore(String consumerName) {
        ignorableConsumers.add(consumerName);
        return this;
      }

      @Override
      public Future<RecordMetadata> go() {
        if (this.topic == null) {
          throw new RuntimeException("topic == null");
        }

        Box box = new Box();
        box.body = body;
        box.author = source.author();
        box.ignorableConsumers = ignorableConsumers.stream().sorted().collect(Collectors.toList());

        String key = source.extractKey(body);

        return getProducer().send(new ProducerRecord<>(topic, partition, timestamp, key, box, headers));
      }
    };
  }

}
