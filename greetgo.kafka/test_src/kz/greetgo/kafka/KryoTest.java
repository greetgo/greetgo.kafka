package kz.greetgo.kafka;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import kz.greetgo.kafka.util.HexUtil;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;

import static org.fest.assertions.api.Assertions.assertThat;

public class KryoTest {

  @Test
  public void asd() {

    ModelKryo modelKryo = new ModelKryo();
    modelKryo.name = "asd WOW Привет!!!";
    modelKryo.age = 456;
    modelKryo.wow = 234999L;
    ModelKryo hello = new ModelKryo();
    hello.name = "Помидор";
    modelKryo.hello = hello;

    Kryo kryo = new Kryo();
    kryo.register(ModelKryo.class);
    kryo.register(Object.class);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try (Output output = new Output(outputStream)) {
      kryo.writeObject(output, modelKryo);
    }

    String hex = HexUtil.bytesToHex(outputStream.toByteArray());

    System.out.println(hex);

    Input input = new Input(HexUtil.hexToBytes(hex));
    ModelKryo actual = kryo.readObject(input, ModelKryo.class);

    assertThat(actual.name).isEqualTo(modelKryo.name);
    assertThat(actual.age).isEqualTo(modelKryo.age);
    assertThat(actual.wow).isEqualTo(modelKryo.wow);
    assertThat(actual.hello).isInstanceOf(ModelKryo.class);
    assertThat(((ModelKryo) actual.hello).name).isEqualTo("Помидор");
  }
}
