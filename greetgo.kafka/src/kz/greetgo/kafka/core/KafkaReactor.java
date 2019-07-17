package kz.greetgo.kafka.core;

import kz.greetgo.kafka.consumer.ConsumerReactor;
import kz.greetgo.kafka.core.config.EventConfigStorage;
import kz.greetgo.kafka.core.logger.LoggerExternal;
import kz.greetgo.kafka.producer.ProducerFacade;
import kz.greetgo.strconverter.StrConverter;

import java.util.Optional;
import java.util.function.Supplier;

public interface KafkaReactor {

  String DEFAULT_INNER_PRODUCER_NAME = "default_inner_producer";

  LoggerExternal logger();

  void setConsumerConfigStorage(EventConfigStorage configStorage);

  void setProducerConfigStorage(EventConfigStorage configStorage);

  void setAuthorSupplier(Supplier<String> authorSupplier);

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
