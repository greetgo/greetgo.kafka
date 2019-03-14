package kz.greetgo.kafka2.producer;

import com.esotericsoftware.kryo.Kryo;
import kz.greetgo.kafka2.model.Box;
import kz.greetgo.kafka2.serializer.BoxSerializer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Map;

public interface ProducerSource {
  Kryo getKryo();

  byte[] extractKey(Object object);

  String author();

  Producer<byte[], Box> createProducer(String producerName,
                                       ByteArraySerializer keySerializer,
                                       BoxSerializer valueSerializer);
}
