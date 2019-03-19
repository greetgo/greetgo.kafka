package kz.greetgo.kafka.core;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;
import kz.greetgo.kafka.consumer.ConsumerDefinition;
import kz.greetgo.kafka.consumer.ConsumerDefinitionExtractor;
import kz.greetgo.kafka.consumer.ConsumerLogger;
import kz.greetgo.kafka.core.config.EventConfigStorage;
import kz.greetgo.kafka.errors.NotDefined;
import kz.greetgo.kafka.model.Box;
import kz.greetgo.kafka.producer.ProducerFacade;
import kz.greetgo.kafka.producer.ProducerSource;
import kz.greetgo.kafka.util.EmptyConsumerLogger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

public abstract class KafkaReactorAbstract implements KafkaReactor {
  protected EventConfigStorage configStorage;
  protected Supplier<String> authorGetter;
  protected String producerConfigRootPath;
  protected ConsumerLogger consumerLogger = new EmptyConsumerLogger();
  protected String hostId;
  protected Supplier<String> bootstrapServers;

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
    //noinspection ArraysAsListWithZeroOrOneArgument
    kryo.register(Arrays.asList().getClass(), new CollectionSerializer() {
      @Override
      protected Collection create(Kryo kryo, Input input, Class type, int size) {
        return new ArrayList();
      }
    });
  }

  @Override
  public Kryo getKryo() {
    return kryo;
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
