package kz.greetgo.kafka2;

import kz.greetgo.kafka2.core.HasStrKafkaKey;

import java.io.File;
import java.nio.file.Files;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ModelKryo2 implements HasStrKafkaKey {
  public Long id;
  public String surname;
  public int loaderCount;

  @Override
  public String toString() {
    return "ModelKryo2{id=" + id + ", surname='" + surname + "', loaderCount=" + loaderCount + '}';
  }

  @Override
  public String extractStrKafkaKey() {
    return surname;
  }

  @SuppressWarnings("unused")
  public static ModelKryo2 readFromFile(File file) throws Exception {
    String content = new String(Files.readAllBytes(file.toPath()), UTF_8);

    ModelKryo2 ret = new ModelKryo2();

    for (String line : content.split(";")) {
      int idx = line.indexOf('=');
      if (idx < 0) {
        continue;
      }

      String key = line.substring(0, idx).trim();
      String value = line.substring(idx + 1).trim();

      switch (key) {
        case "surname":
          ret.surname = value;
          break;
        case "loaderCount":
          ret.loaderCount = Integer.parseInt(value);
          break;
        case "id":
          ret.id = Long.valueOf(value);
          break;
      }
    }

    return ret;
  }
}
