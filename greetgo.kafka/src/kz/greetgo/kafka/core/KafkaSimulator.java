package kz.greetgo.kafka.core;

import com.esotericsoftware.kryo.Kryo;
import kz.greetgo.kafka.consumer.ConsumerDefinition;
import kz.greetgo.kafka.model.Box;
import kz.greetgo.kafka.model.BoxHolder;
import kz.greetgo.kafka.producer.ProducerSource;
import kz.greetgo.kafka.serializer.BoxSerializer;
import kz.greetgo.kafka.util.BoxUtil;
import kz.greetgo.kafka.util.KeyUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static java.util.Collections.synchronizedList;
import static java.util.Collections.unmodifiableList;
import static jdk.nashorn.internal.runtime.JSType.toLong;

public class KafkaSimulator extends KafkaReactorAbstract {

  @Override
  public void stopConsumers() {}

  @Override
  protected ProducerSource getProducerSource() {
    return producerSource;
  }

  private final ConcurrentHashMap<String, MockProducerHolder> producers = new ConcurrentHashMap<>();

  private final ProducerSource producerSource = new ProducerSource() {
    @Override
    public Kryo getKryo() {
      return kryo;
    }

    @Override
    public byte[] extractKey(Object object) {
      return KeyUtil.extractKey(object);
    }

    @Override
    public String author() {
      return authorGetter == null ? null : authorGetter.get();
    }

    @Override
    public Producer<byte[], Box> createProducer(String producerName,
                                                ByteArraySerializer keySerializer,
                                                BoxSerializer valueSerializer) {

      if (producers.containsKey(producerName)) {
        throw new RuntimeException("Producer with name = " + producerName
          + " already created. Please select another name");
      }

      MockProducerHolder mockProducerHolder = new MockProducerHolder(
        producerName, keySerializer, valueSerializer, getCluster()
      );
      producers.put(mockProducerHolder.getProducerName(), mockProducerHolder);
      return mockProducerHolder.getProducer();

    }
  };

  protected Cluster getCluster() {
    return Cluster.empty();
  }

  public void push() {

    for (MockProducerHolder producer : producers.values()) {
      producer.getProducer().flush();
      List<ProducerRecord<byte[], Box>> history = producer.getProducer().history();
      producer.getProducer().clear();

      for (ProducerRecord<byte[], Box> record : history) {
        pushRecord(record, producer);
      }
    }

  }

  private final List<ConsumerRecord<byte[], Box>> pushedRecords = synchronizedList(new ArrayList<>());

  private void pushRecord(ProducerRecord<byte[], Box> r, MockProducerHolder producer) {

    TopicPartition topicPartition = producer.topicPartition(r);

    ConsumerRecord<byte[], Box> consumerRecord = new ConsumerRecord<>(
      topicPartition.topic(), topicPartition.partition(), 1L,
      toLong(r.timestamp()), TimestampType.CREATE_TIME, 1L, 1, 1, r.key(), r.value(), r.headers()
    );

    List<ConsumerDefinition> consumerDefinitionList = this.consumerDefinitionList;

    if (consumerDefinitionList != null) {

      Map<TopicPartition, List<ConsumerRecord<byte[], Box>>> map = new HashMap<>();
      map.put(topicPartition, singletonList(consumerRecord));

      ConsumerRecords<byte[], Box> singleList = new ConsumerRecords<>(map);

      for (ConsumerDefinition consumerDefinition : consumerDefinitionList) {
        if (!consumerDefinition.invoke(singleList)) {
          throw new RuntimeException("Cannot invoke consumer " + consumerDefinition.logDisplay()
            + " of record " + r.value());
        }
      }

    }

    pushedRecords.add(consumerRecord);
  }

  @SuppressWarnings("unused")
  public void clearAllProducers() {
    for (MockProducerHolder producer : producers.values()) {
      producer.getProducer().clear();
    }
  }

  @SuppressWarnings("unused")
  public void clearPushed() {
    pushedRecords.clear();
  }

  @SuppressWarnings("unused")
  public List<ConsumerRecord<byte[], Box>> allPushed() {
    return unmodifiableList(new ArrayList<>(pushedRecords));
  }

  @SuppressWarnings("unused")
  public <T> List<BoxHolder<T>> pushedOf(Class<T> aClass) {

    return pushedRecords
      .stream()
      .map(rec -> BoxUtil.hold(rec.value(), aClass))
      .filter(Optional::isPresent)
      .map(Optional::get)
      .collect(Collectors.toList())
      ;

  }

  private List<ConsumerDefinition> consumerDefinitionList;

  @Override
  public void startConsumers() {
    verifyControllerList();
    consumerDefinitionList = accumulateConsumerDefinitionList();
  }
}
