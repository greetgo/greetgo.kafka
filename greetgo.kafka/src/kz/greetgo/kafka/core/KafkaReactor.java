package kz.greetgo.kafka.core;

import kz.greetgo.kafka.consumer.ConsumerReactor;
import kz.greetgo.kafka.core.config.EventConfigStorage;
import kz.greetgo.kafka.core.logger.LoggerExternal;
import kz.greetgo.kafka.producer.ProducerFacade;
import kz.greetgo.strconverter.StrConverter;

import java.util.Optional;
import java.util.function.Supplier;

public interface KafkaReactor {

  LoggerExternal logger();

  void setProducerConfigRootPath(String producerConfigRootPath);

  void setAuthorSupplier(Supplier<String> authorSupplier);

  void setConfigStorage(EventConfigStorage configStorage);

  void setBootstrapServers(Supplier<String> bootstrapServers);

  void setStrConverterSupplier(Supplier<StrConverter> strConverterSupplier);

  default void addControllers(Iterable<Object> controllers) {
    controllers.forEach(this::addController);
  }

  void addController(Object controller);

  void setHostId(String hostId);

  void startConsumers();

  void stopConsumers();

  ProducerFacade createProducer(String producerName);

  Optional<ConsumerReactor> consumer(String consumerName);

}
