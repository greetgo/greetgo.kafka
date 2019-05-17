package kz.greetgo.kafka.core;

import kz.greetgo.kafka.consumer.ConsumerDefinition;
import kz.greetgo.kafka.consumer.ConsumerDefinitionExtractor;
import kz.greetgo.kafka.core.config.EventConfigStorage;
import kz.greetgo.kafka.core.logger.Logger;
import kz.greetgo.kafka.core.logger.LoggerExternal;
import kz.greetgo.kafka.errors.NotDefined;
import kz.greetgo.kafka.model.Box;
import kz.greetgo.kafka.producer.ProducerFacade;
import kz.greetgo.kafka.producer.ProducerSource;
import kz.greetgo.strconverter.StrConverter;
import kz.greetgo.strconverter.simple.StrConverterSimple;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.synchronizedList;

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
  public void setAuthorGetter(Supplier<String> authorGetter) {
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

  private final StrConverter strConverter = new StrConverterSimple();

  {
    strConverter.useClass(Box.class);
    strConverter.useClass(ArrayList.class);
  }

  @Override
  public StrConverter getReactorStrConverter() {
    return strConverter;
  }

  @Override
  public void registerStrConverterPreparation(StrConverterPreparation strConverterPreparation) {
    registeredStrConverterPreparations.add(strConverterPreparation);
  }

  private final List<StrConverterPreparation> registeredStrConverterPreparations = synchronizedList(new ArrayList<>());

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
