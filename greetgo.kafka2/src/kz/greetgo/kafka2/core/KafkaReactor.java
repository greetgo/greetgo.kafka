package kz.greetgo.kafka2.core;

import kz.greetgo.kafka2.consumer.ConsumerLogger;
import kz.greetgo.kafka2.core.config.ConfigStorage;
import kz.greetgo.kafka2.producer.ProducerFacade;

import java.util.function.Supplier;

public interface KafkaReactor {

  void setAuthorGetter(Supplier<String> authorGetter);

  void setConfigStorage(ConfigStorage configStorage);

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
