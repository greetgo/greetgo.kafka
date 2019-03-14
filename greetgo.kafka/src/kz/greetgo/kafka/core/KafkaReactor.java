package kz.greetgo.kafka.core;

import kz.greetgo.kafka.consumer.ConsumerLogger;
import kz.greetgo.kafka.core.config.EventConfigStorage;
import kz.greetgo.kafka.producer.ProducerFacade;

import java.util.function.Supplier;

public interface KafkaReactor {

  void setProducerConfigRootPath(String producerConfigRootPath);

  void setAuthorGetter(Supplier<String> authorGetter);

  void setConfigStorage(EventConfigStorage configStorage);

  void setConsumerLogger(ConsumerLogger consumerLogger);

  void setBootstrapServers(Supplier<String> bootstrapServers);

  default void addControllers(Iterable<Object> controllers) {
    controllers.forEach(this::addController);
  }

  void addController(Object controller);

  default void registerModels(Iterable<Class<?>> modelClasses) {
    modelClasses.forEach(this::registerModel);
  }

  void registerModel(Class<?> modelClass);

  void setHostId(String hostId);

  void startConsumers();

  void stopConsumers();

  ProducerFacade createProducer(String producerName);
}
