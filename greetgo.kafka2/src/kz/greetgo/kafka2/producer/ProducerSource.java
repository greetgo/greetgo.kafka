package kz.greetgo.kafka2.producer;

import com.esotericsoftware.kryo.Kryo;

import java.util.Map;

public interface ProducerSource {
  Kryo getKryo();

  byte[] extractKey(Object object);

  String author();

  Map<String, Object> producerConfig(String producerName);
}
