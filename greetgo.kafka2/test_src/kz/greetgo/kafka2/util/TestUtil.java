package kz.greetgo.kafka2.util;

import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public class TestUtil {

  public static byte[] linesToBytes(List<String> lines) {
    return String.join("\n", lines).getBytes(UTF_8);
  }

}
