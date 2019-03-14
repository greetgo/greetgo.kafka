package kz.greetgo.kafka.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import kz.greetgo.kafka.model.Box;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class BoxDeserializer implements Deserializer<Box> {

  private final Kryo kryo;

  public BoxDeserializer(Kryo kryo) {
    this.kryo = kryo;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public Box deserialize(String topic, byte[] data) {
    try (Input input = new Input(data)) {
      return kryo.readObject(input, Box.class);
    }
  }

  @Override
  public void close() {}

}
