package kz.greetgo.kafka2.producer;

import com.esotericsoftware.kryo.Kryo;

import java.util.Map;

public interface ProducerSource {
  Kryo getKryo();

  String bootstrapServers();

  String extractKey(Object body);

  String author();

  default void performAdditionalProducerConfiguration(Map<String, Object> config) {}
}
