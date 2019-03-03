package kz.greetgo.kafka2.core;

import com.esotericsoftware.kryo.Kryo;
import kz.greetgo.kafka2.consumer.ConsumerDefinition;
import kz.greetgo.kafka2.consumer.ConsumerDefinitionExtractor;
import kz.greetgo.kafka2.consumer.ConsumerLogger;
import kz.greetgo.kafka2.consumer.ConsumerReactor;
import kz.greetgo.kafka2.core.config.ConfigStorage;
import kz.greetgo.kafka2.errors.CannotExtractKeyFrom;
import kz.greetgo.kafka2.errors.NotDefined;
import kz.greetgo.kafka2.model.Box;
import kz.greetgo.kafka2.producer.ProducerConfigWorker;
import kz.greetgo.kafka2.producer.ProducerFacade;
import kz.greetgo.kafka2.producer.ProducerSource;
import kz.greetgo.kafka2.util.EmptyConsumerLogger;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class KafkaReactorImpl implements KafkaReactor {
  private ConfigStorage configStorage;
  private Supplier<String> bootstrapServers;
  private Supplier<String> authorGetter = null;
  private ConsumerLogger consumerLogger = new EmptyConsumerLogger();

  public void setAuthorGetter(Supplier<String> authorGetter) {
    this.authorGetter = authorGetter;
  }

  @Override
  public void setConfigStorage(ConfigStorage configStorage) {
    this.configStorage = configStorage;
  }

  @Override
  public void setConsumerLogger(ConsumerLogger consumerLogger) {
    this.consumerLogger = consumerLogger;
  }

  @Override
  public void setBootstrapServers(Supplier<String> bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }

  private final List<Object> controllerList = new ArrayList<>();

  @Override
  public void addController(Object controller) {
    controllerList.add(controller);
  }

  private final Kryo kryo = new Kryo();

  {
    kryo.register(Box.class);
  }

  @Override
  public void registerModel(Class<?> modelClass) {
    kryo.register(modelClass);
  }

  private final List<ConsumerReactor> consumerReactorList = new ArrayList<>();

  @Override
  public void startConsumers() {
    if (configStorage == null) {
      throw new NotDefined("configStorage in " + KafkaReactor.class.getSimpleName() + ".start()");
    }
    if (bootstrapServers == null) {
      throw new NotDefined("bootstrapServers in " + KafkaReactor.class.getSimpleName() + ".start()");
    }
    if (controllerList.isEmpty()) {
      throw new NotDefined("Controllers in " + KafkaReactor.class.getSimpleName() + ".start()");
    }

    List<ConsumerDefinition> consumerDefinitionList = new ArrayList<>();

    for (Object controller : controllerList) {
      consumerDefinitionList.addAll(ConsumerDefinitionExtractor.extract(controller, consumerLogger));
    }

    if (consumerDefinitionList.isEmpty()) {
      throw new NotDefined("Consumers in " + KafkaReactor.class.getSimpleName() + ".start()");
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

  private final ProducerSource producerSource = new ProducerSource() {
    @Override
    public Kryo getKryo() {
      return kryo;
    }

    @Override
    public byte[] extractKey(Object object) {
      if (object instanceof HasByteArrayKafkaKey) {
        return ((HasByteArrayKafkaKey) object).extractByteArrayKafkaKey();
      }
      if (object instanceof HasStrKafkaKey) {
        String str = ((HasStrKafkaKey) object).extractStrKafkaKey();
        if (str == null) {
          return new byte[0];
        }
        return str.getBytes(StandardCharsets.UTF_8);
      }
      throw new CannotExtractKeyFrom(object);
    }

    @Override
    public String author() {
      return authorGetter == null ? null : authorGetter.get();
    }

    private final ProducerConfigWorker producerConfigWorker = new ProducerConfigWorker(() -> configStorage);

    @Override
    public Map<String, Object> producerConfig(String producerName) {
      Map<String, Object> configMap = producerConfigWorker.getConfigFor(producerName);
      configMap.put("bootstrap.servers", bootstrapServers.get());
      return configMap;
    }
  };

  @Override
  public ProducerFacade createProducer(String producerName) {
    return new ProducerFacade(producerName, producerSource);
  }
}
