package kz.greetgo.kafka.util;

import kz.greetgo.kafka.core.HasByteArrayKafkaKey;
import kz.greetgo.kafka.core.HasStrKafkaKey;
import kz.greetgo.kafka.errors.CannotExtractKeyFrom;

import java.nio.charset.StandardCharsets;

public class KeyUtil {
  public static byte[] extractKey(Object object) {
    if (object instanceof HasByteArrayKafkaKey) {
      return ((HasByteArrayKafkaKey) object).extractByteArrayKafkaKey();
    }
    if (object instanceof HasStrKafkaKey) {
      String str = ((HasStrKafkaKey) object).extractStrKafkaKey();
      if (str == null) {
        return new byte[0];
      }
      return str.getBytes(StandardCharsets.UTF_8);
    }
    throw new CannotExtractKeyFrom(object);
  }
}
