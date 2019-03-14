package kz.greetgo.kafka2.core;

import com.esotericsoftware.kryo.Kryo;
import kz.greetgo.kafka2.consumer.ConsumerDefinition;
import kz.greetgo.kafka2.consumer.ConsumerDefinitionExtractor;
import kz.greetgo.kafka2.consumer.ConsumerLogger;
import kz.greetgo.kafka2.core.config.ConfigStorage;
import kz.greetgo.kafka2.errors.NotDefined;
import kz.greetgo.kafka2.model.Box;
import kz.greetgo.kafka2.producer.ProducerFacade;
import kz.greetgo.kafka2.producer.ProducerSource;
import kz.greetgo.kafka2.util.EmptyConsumerLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public abstract class KafkaReactorAbstract implements KafkaReactor {
  protected ConfigStorage configStorage;
  protected Supplier<String> authorGetter;
  protected String producerConfigRootPath;
  protected ConsumerLogger consumerLogger = new EmptyConsumerLogger();
  protected String hostId;
  protected Supplier<String> bootstrapServers;

  @Override
  public void setConfigStorage(ConfigStorage configStorage) {
    this.configStorage = configStorage;
  }

  @Override
  public void setAuthorGetter(Supplier<String> authorGetter) {
    this.authorGetter = authorGetter;
  }

  @Override
  public void setProducerConfigRootPath(String producerConfigRootPath) {
    this.producerConfigRootPath = producerConfigRootPath;
  }

  @Override
  public void setConsumerLogger(ConsumerLogger consumerLogger) {
    this.consumerLogger = consumerLogger;
  }

  @Override
  public void setBootstrapServers(Supplier<String> bootstrapServers) {this.bootstrapServers = bootstrapServers;}

  protected final List<Object> controllerList = new ArrayList<>();

  @Override
  public void addController(Object controller) {
    controllerList.add(controller);
  }

  protected final Kryo kryo = new Kryo();

  {
    kryo.register(Box.class);
    kryo.register(ArrayList.class);
  }


  @Override
  public void registerModel(Class<?> modelClass) {
    kryo.register(modelClass);
  }

  @Override
  public void setHostId(String hostId) {
    this.hostId = hostId;
  }

  protected abstract ProducerSource getProducerSource();

  @Override
  public ProducerFacade createProducer(String producerName) {
    return new ProducerFacade(producerName, getProducerSource());
  }

  protected void verifyControllerList() {
    if (controllerList.isEmpty()) {
      throw new NotDefined("controllerList in " + this.getClass().getSimpleName());
    }
  }

  protected List<ConsumerDefinition> accumulateConsumerDefinitionList() {
    List<ConsumerDefinition> consumerDefinitionList = new ArrayList<>();

    ConsumerDefinitionExtractor cde = new ConsumerDefinitionExtractor();
    cde.consumerLogger = consumerLogger;
    cde.hostId = hostId;

    for (Object controller : controllerList) {
      consumerDefinitionList.addAll(cde.extract(controller));
    }

    if (consumerDefinitionList.isEmpty()) {
      throw new NotDefined("Consumers in " + KafkaReactor.class.getSimpleName() + ".start()");
    }

    return consumerDefinitionList;
  }
}
