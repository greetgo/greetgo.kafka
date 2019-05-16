package kz.greetgo.kafka.core;

import com.esotericsoftware.kryo.Kryo;
import kz.greetgo.kafka.core.config.EventConfigStorage;
import kz.greetgo.kafka.core.logger.LoggerExternal;
import kz.greetgo.kafka.producer.ProducerFacade;

import java.util.function.Supplier;

public interface KafkaReactor {

  LoggerExternal logger();

  void setProducerConfigRootPath(String producerConfigRootPath);

  void setAuthorGetter(Supplier<String> authorGetter);

  void setConfigStorage(EventConfigStorage configStorage);

  void setBootstrapServers(Supplier<String> bootstrapServers);

  Kryo getReactorKryo();

  default void addControllers(Iterable<Object> controllers) {
    controllers.forEach(this::addController);
  }

  void addController(Object controller);

  void registerKryoPreparation(KryoPreparation kryoPreparation);

  void setHostId(String hostId);

  void startConsumers();

  void stopConsumers();

  ProducerFacade createProducer(String producerName);

}
