package kz.greetgo.kafka.producer;

import kz.greetgo.kafka.core.logger.Logger;
import kz.greetgo.kafka.model.Box;
import kz.greetgo.kafka.serializer.BoxSerializer;
import kz.greetgo.strconverter.StrConverter;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Map;

public interface ProducerSource {

  StrConverter getStrConverter();

  Logger logger();

  byte[] extractKey(Object object);

  String author();

  long getProducerConfigUpdateTimestamp(String producerName);

  Map<String, Object> getConfigFor(String producerName);

  Producer<byte[], Box> createProducer(String producerName,
                                       ByteArraySerializer keySerializer,
                                       BoxSerializer valueSerializer);

}
