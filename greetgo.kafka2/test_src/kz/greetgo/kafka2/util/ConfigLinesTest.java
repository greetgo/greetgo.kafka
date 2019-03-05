package kz.greetgo.kafka2.util;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static kz.greetgo.kafka2.util.TestUtil.linesToBytes;
import static org.fest.assertions.api.Assertions.assertThat;

public class ConfigLinesTest {

  @Test
  public void fromBytes() {

    List<String> lines = new ArrayList<>();
    lines.add("key001=123");
    lines.add("#key002=321");
    lines.add("key002=334455");

    ConfigLines configLines = ConfigLines.fromBytes(linesToBytes(lines), "wow/config.txt");

    String key001value = configLines.getValue("key001");
    String key002value = configLines.getValue("key002");

    assertThat(key001value).isEqualTo("123");
    assertThat(key002value).isEqualTo("334455");

  }

  @Test
  public void addValueVariant() {

    List<String> lines = new ArrayList<>();
    lines.add("  key001  = 123    ");
    lines.add("# key002  = 321    ");
    lines.add("  key002  = 334455 ");
    lines.add("  key003  = oops   ");

    ConfigLines configLines = ConfigLines.fromBytes(linesToBytes(lines), "wow/config.txt");

    assertThat(configLines.isModified()).isFalse();

    configLines.addValueVariant("key002", "321");

    assertThat(configLines.isModified()).isFalse();

    configLines.addValueVariant("key002", "variant1");

    assertThat(configLines.isModified()).isTrue();

    configLines.addValueVariant("key002", "variant2");

    assertThat(configLines.isModified()).isTrue();

    List<String> list = Arrays.asList(new String(configLines.toBytes(), UTF_8).split("\n"));

    assertThat(list.get(0)).isEqualTo("  key001  = 123    ");
    assertThat(list.get(1)).isEqualTo("# key002  = 321    ");
    assertThat(list.get(2)).isEqualTo("  key002  = 334455 ");
    assertThat(list.get(3)).isEqualTo("#key002=variant1");
    assertThat(list.get(4)).isEqualTo("#key002=variant2");
    assertThat(list.get(5)).isEqualTo("  key003  = oops   ");
  }
}
