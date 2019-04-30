package kz.greetgo.kafka.producer;

import com.esotericsoftware.kryo.Kryo;
import kz.greetgo.kafka.core.logger.Logger;
import kz.greetgo.kafka.model.Box;
import kz.greetgo.kafka.serializer.BoxSerializer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public interface ProducerSource {

  Logger logger();

  Kryo getKryo();

  byte[] extractKey(Object object);

  String author();

  Producer<byte[], Box> createProducer(String producerName,
                                       ByteArraySerializer keySerializer,
                                       BoxSerializer valueSerializer);
}
