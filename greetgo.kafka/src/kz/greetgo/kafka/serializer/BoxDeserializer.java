package kz.greetgo.kafka.serializer;

import kz.greetgo.kafka.model.Box;
import kz.greetgo.strconverter.StrConverter;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class BoxDeserializer implements Deserializer<Box> {

  private final StrConverter strConverter;

  public BoxDeserializer(StrConverter strConverter) {
    this.strConverter = strConverter;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public Box deserialize(String topic, byte[] data) {

    if (data == null) {
      return null;
    }

    String str = new String(data, StandardCharsets.UTF_8);

    return strConverter.fromStr(str);

  }

  @Override
  public void close() {}

}
