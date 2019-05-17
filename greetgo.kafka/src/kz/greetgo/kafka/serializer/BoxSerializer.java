package kz.greetgo.kafka.serializer;

import kz.greetgo.kafka.model.Box;
import kz.greetgo.strconverter.StrConverter;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public class BoxSerializer implements Serializer<Box> {

  private final StrConverter strConverter;

  public BoxSerializer(StrConverter strConverter) {
    this.strConverter = strConverter;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public void close() {}

  @Override
  public byte[] serialize(String topic, Box data) {

    String str = strConverter.toStr(data);

    return str.getBytes(UTF_8);

  }
}
