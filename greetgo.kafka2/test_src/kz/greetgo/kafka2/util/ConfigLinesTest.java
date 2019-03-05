package kz.greetgo.kafka2.util;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.fest.assertions.api.Assertions.assertThat;

public class ConfigLinesTest {

  @Test
  public void fromBytes() {

    List<String> lines = new ArrayList<>();
    lines.add("key001=123");
    lines.add("#key002=321");
    lines.add("key002=334455");

    byte[] bytes = String.join("\n", lines).getBytes(UTF_8);

    ConfigLines configLines = ConfigLines.fromBytes(bytes, "wow/config.txt");

    String key001value = configLines.getValue("key001");
    String key002value = configLines.getValue("key002");

    assertThat(key001value).isEqualTo("123");
    assertThat(key002value).isEqualTo("334455");
  }
}
