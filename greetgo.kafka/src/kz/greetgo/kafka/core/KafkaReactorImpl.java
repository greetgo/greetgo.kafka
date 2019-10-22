package kz.greetgo.kafka.core;

import kz.greetgo.kafka.consumer.ConsumerConfigDefaults;
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
import kz.greetgo.kafka.util.ConfigLines;
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

  public ConsumerConfigDefaults consumerConfigDefaults = ConsumerConfigDefaults.withDefaults();

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
      consumerReactor.producerSource = getProducerSource();
      consumerReactor.hostId = hostId;
      consumerReactor.consumerConfigDefaults = () -> consumerConfigDefaults;
      consumerReactor.start();
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

  public void joinToConsumers() {
    consumerReactorList.forEach(ConsumerReactorImpl::join);
  }

  private final ProducerConfigWorker producerConfigWorker = new ProducerConfigWorker(
    () -> producerConfigStorage, this::putProducerDefaultValues
  );

  protected void putProducerDefaultValues(ConfigLines configLines) {
    configLines.putValue("prod.acts                    ", "all");
    configLines.putValue("prod.buffer.memory           ", "33554432");
    configLines.putValue("prod.compression.type        ", "none");
    configLines.putValue("prod.batch.size              ", "16384");
    configLines.putValue("prod.connections.max.idle.ms ", "540000");
    configLines.putValue("prod.request.timeout.ms      ", "30000");
    configLines.putValue("prod.linger.ms               ", "1");
    configLines.putValue("prod.batch.size              ", "16384");

    configLines.putValue("prod.retries                               ", "2147483647");
    configLines.putValue("prod.max.in.flight.requests.per.connection ", "1");
    configLines.putValue("prod.delivery.timeout.ms                   ", "35000");
  }

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
    public Map<String, Object> getConfigFor(String producerName) {
      return producerConfigWorker.getConfigFor(producerName);
    }

    @Override
    public Producer<byte[], Box> createProducer(String producerName,
                                                ByteArraySerializer keySerializer,
                                                BoxSerializer valueSerializer) {

      Map<String, Object> configMap = getConfigFor(producerName);
      configMap.put("bootstrap.servers", bootstrapServers.get());
      if (logger.isShow(LoggerType.SHOW_PRODUCER_CONFIG)) {
        logger.logProducerConfigOnCreating(producerName, configMap);
      }
      return new KafkaProducer<>(configMap, keySerializer, valueSerializer);

    }
  };
}
