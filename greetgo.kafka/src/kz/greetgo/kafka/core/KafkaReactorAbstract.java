package kz.greetgo.kafka.core;

import kz.greetgo.kafka.consumer.ConsumerDefinition;
import kz.greetgo.kafka.consumer.ConsumerDefinitionExtractor;
import kz.greetgo.kafka.consumer.ConsumerReactor;
import kz.greetgo.kafka.core.config.EventConfigStorage;
import kz.greetgo.kafka.core.logger.Logger;
import kz.greetgo.kafka.core.logger.LoggerExternal;
import kz.greetgo.kafka.errors.NotDefined;
import kz.greetgo.kafka.producer.ProducerFacade;
import kz.greetgo.kafka.producer.ProducerSource;
import kz.greetgo.strconverter.StrConverter;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

public abstract class KafkaReactorAbstract implements KafkaReactor {
  protected EventConfigStorage configStorage;
  protected Supplier<String> authorGetter;
  protected String producerConfigRootPath;

  protected String hostId;
  protected Supplier<String> bootstrapServers;

  protected final Logger logger = new Logger();

  @Override
  public LoggerExternal logger() {
    return logger;
  }

  @Override
  public void setConfigStorage(EventConfigStorage configStorage) {
    this.configStorage = configStorage;
  }

  @Override
  public void setAuthorSupplier(Supplier<String> authorGetter) {
    this.authorGetter = authorGetter;
  }

  @Override
  public void setProducerConfigRootPath(String producerConfigRootPath) {
    this.producerConfigRootPath = producerConfigRootPath;
  }

  @Override
  public void setBootstrapServers(Supplier<String> bootstrapServers) {this.bootstrapServers = bootstrapServers;}

  protected final List<Object> controllerList = new ArrayList<>();

  @Override
  public void addController(Object controller) {
    controllerList.add(controller);
  }

  private Supplier<StrConverter> strConverterSupplier;

  protected Supplier<StrConverter> strConverterSupplier() {

    Supplier<StrConverter> ret = strConverterSupplier;

    if (ret == null) {
      throw new NullPointerException("strConverterSupplier == null");
    }

    return ret;

  }

  @Override
  public Optional<ConsumerReactor> consumer(String consumerName) {
    return Optional.empty();
  }

  @Override
  public void setStrConverterSupplier(Supplier<StrConverter> strConverterSupplier) {
    this.strConverterSupplier = strConverterSupplier;
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
    cde.logger = logger;
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
