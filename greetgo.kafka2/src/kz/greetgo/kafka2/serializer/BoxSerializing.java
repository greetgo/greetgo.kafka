package kz.greetgo.kafka2.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import kz.greetgo.kafka2.model.Box;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.util.Map;

public class BoxSerializing implements Serializer<Box> {

  private final Kryo kryo;

  public BoxSerializing(Kryo kryo) {
    this.kryo = kryo;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public void close() {}

  @Override
  public byte[] serialize(String topic, Box data) {

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try (Output output = new Output(outputStream)) {
      kryo.writeObject(output, data);
    }

    return outputStream.toByteArray();
  }
}
