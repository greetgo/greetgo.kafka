package kz.greetgo.kafka.core;

import kz.greetgo.kafka.consumer.ConsumerDefinition;
import kz.greetgo.kafka.consumer.ConsumerReactor;
import kz.greetgo.kafka.consumer.ConsumerReactorImpl;
import kz.greetgo.kafka.core.logger.Logger;
import kz.greetgo.kafka.core.logger.LoggerType;
import kz.greetgo.kafka.errors.NotDefined;
import kz.greetgo.kafka.model.Box;
import kz.greetgo.kafka.producer.ProducerConfigWorker;
import kz.greetgo.kafka.producer.ProducerSource;
import kz.greetgo.kafka.serializer.BoxSerializer;
import kz.greetgo.kafka.util.KeyUtil;
import kz.greetgo.strconverter.StrConverter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class KafkaReactorImpl extends KafkaReactorAbstract {

  private final List<ConsumerReactorImpl> consumerReactorList = new ArrayList<>();

  @Override
  public void startConsumers() {
    verifyControllerList();
    List<ConsumerDefinition> consumerDefinitionList = accumulateConsumerDefinitionList();

    if (consumerConfigStorage == null) {
      throw new NotDefined("consumerConfigStorage in " + getClass().getSimpleName() + ".startConsumers()");
    }
    if (bootstrapServers == null) {
      throw new NotDefined("bootstrapServers in " + getClass().getSimpleName() + ".startConsumers()");
    }


    for (ConsumerDefinition consumerDefinition : consumerDefinitionList) {
      ConsumerReactorImpl consumerReactor = new ConsumerReactorImpl();
      consumerReactorList.add(consumerReactor);
      consumerReactor.logger = logger;
      consumerReactor.strConverterSupplier = strConverterSupplier();
      consumerReactor.bootstrapServers = bootstrapServers;
      consumerReactor.configStorage = consumerConfigStorage;
      consumerReactor.consumerDefinition = consumerDefinition;
      consumerReactor.hostId = hostId;
      consumerReactor.start();
      System.out.println("5hb4326v6 :: started consumer " + consumerDefinition);
    }
  }

  @Override
  public Optional<ConsumerReactor> consumer(String consumerName) {
    for (ConsumerReactorImpl consumerReactor : consumerReactorList) {
      if (consumerReactor.consumerDefinition.getConsumerName().equals(consumerName)) {
        return Optional.of(consumerReactor);
      }
    }
    return Optional.empty();
  }

  @Override
  public void stopConsumers() {

    consumerReactorList.forEach(ConsumerReactorImpl::stop);
    consumerReactorList.clear();

    producerConfigWorker.close();

  }

  private final ProducerConfigWorker producerConfigWorker = new ProducerConfigWorker(() -> producerConfigStorage);

  @Override
  public ProducerSource getProducerSource() {
    return producerSource;
  }

  private final ProducerSource producerSource = new ProducerSource() {

    @Override
    public Logger logger() {
      return logger;
    }

    @Override
    public StrConverter getStrConverter() {
      return strConverterSupplier().get();
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
    public long getProducerConfigUpdateTimestamp(String producerName) {
      return producerConfigWorker.getConfigUpdateTimestamp(producerName);
    }

    @Override
    public Producer<byte[], Box> createProducer(String producerName,
                                                ByteArraySerializer keySerializer,
                                                BoxSerializer valueSerializer) {

      Map<String, Object> configMap = producerConfigWorker.getConfigFor(producerName);
      configMap.put("bootstrap.servers", bootstrapServers.get());
      if (logger.isShow(LoggerType.SHOW_PRODUCER_CONFIG)) {
        logger.logProducerConfigOnCreating(producerName, configMap);
      }
      return new KafkaProducer<>(configMap, keySerializer, valueSerializer);

    }
  };
}
