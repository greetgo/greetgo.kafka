package kz.greetgo.kafka2.core;

import com.esotericsoftware.kryo.Kryo;
import kz.greetgo.kafka2.consumer.ConsumerDefinition;
import kz.greetgo.kafka2.consumer.ConsumerReactor;
import kz.greetgo.kafka2.consumer.annotations.Topic;
import kz.greetgo.kafka2.errors.NotDefined;
import kz.greetgo.kafka2.model.Box;
import kz.greetgo.kafka2.producer.ProducerConfigWorker;
import kz.greetgo.kafka2.producer.ProducerSource;
import kz.greetgo.kafka2.serializer.BoxSerializer;
import kz.greetgo.kafka2.util.KeyUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KafkaReactorImpl extends KafkaReactorAbstract {

  private final List<ConsumerReactor> consumerReactorList = new ArrayList<>();

  @Override
  public void startConsumers() {
    verifyControllerList();
    List<ConsumerDefinition> consumerDefinitionList = accumulateConsumerDefinitionList();

    if (configStorage == null) {
      throw new NotDefined("configStorage in " + KafkaReactor.class.getSimpleName() + ".start()");
    }
    if (bootstrapServers == null) {
      throw new NotDefined("bootstrapServers in " + KafkaReactor.class.getSimpleName() + ".start()");
    }


    for (ConsumerDefinition consumerDefinition : consumerDefinitionList) {
      ConsumerReactor consumerReactor = new ConsumerReactor();
      consumerReactorList.add(consumerReactor);
      consumerReactor.kryo = kryo;
      consumerReactor.bootstrapServers = bootstrapServers;
      consumerReactor.configStorage = configStorage;
      consumerReactor.consumerDefinition = consumerDefinition;
      consumerReactor.consumerLogger = consumerLogger;
      consumerReactor.start();
    }
  }

  @Override
  public void stopConsumers() {
    consumerReactorList.forEach(ConsumerReactor::stop);
    consumerReactorList.clear();
  }

  @Override
  public ProducerSource getProducerSource() {
    return producerSource;
  }

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

    private final ProducerConfigWorker producerConfigWorker = new ProducerConfigWorker(
      () -> producerConfigRootPath, () -> configStorage
    );

    @Override
    public Producer<byte[], Box> createProducer(String producerName,
                                                ByteArraySerializer keySerializer,
                                                BoxSerializer valueSerializer) {

      Map<String, Object> configMap = producerConfigWorker.getConfigFor(producerName);
      configMap.put("bootstrap.servers", bootstrapServers.get());
      return new KafkaProducer<>(configMap, keySerializer, valueSerializer);

    }
  };
}
