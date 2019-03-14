package kz.greetgo.kafka;

import kz.greetgo.kafka.core.HasStrKafkaKey;

import java.io.File;
import java.nio.file.Files;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ModelKryo implements HasStrKafkaKey {
  public String name;
  public int age;
  public Long wow;
  public Object hello;

  @Override
  public String extractStrKafkaKey() {
    return name;
  }

  @Override
  public String toString() {
    return "ModelKryo{name='" + name + "', age=" + age + ", wow=" + wow + ", hello=" + hello + '}';
  }


  @SuppressWarnings("unused")
  public static ModelKryo readFromFile(File file) throws Exception {
    String content = new String(Files.readAllBytes(file.toPath()), UTF_8);

    ModelKryo ret = new ModelKryo();

    for (String line : content.split(";")) {
      int idx = line.indexOf('=');
      if (idx < 0) {
        continue;
      }

      String key = line.substring(0, idx).trim();
      String value = line.substring(idx + 1).trim();

      switch (key) {
        case "name":
          ret.name = value;
          break;
        case "age":
          ret.age = Integer.parseInt(value);
          break;
        case "wow":
          ret.wow = Long.valueOf(value);
          break;
      }
    }

    return ret;
  }
}
