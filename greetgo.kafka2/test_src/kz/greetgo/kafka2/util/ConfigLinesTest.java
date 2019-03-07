package kz.greetgo.kafka2.util;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static kz.greetgo.kafka2.util.TestUtil.linesToBytes;
import static org.fest.assertions.api.Assertions.assertThat;

@SuppressWarnings("ConstantConditions")
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

  private void addValueVariant_testCore(List<String> startLines,
                                        String key, String value, boolean expectedModified,
                                        List<String> expected) {

    ConfigLines configLines = ConfigLines.fromBytes(linesToBytes(startLines), "wow/config.txt");

    //
    //
    configLines.addValueVariant(key, value);
    boolean actualIsModified = configLines.isModified();
    //
    //


    List<String> actual = Arrays.asList(new String(configLines.toBytes(), UTF_8).split("\n"));

    String displayedLists = "\n\nActual lines:" + printLines(actual)
      + "\n\nExpected lines:" + printLines(expected) + "\n\n";

    for (int i = 0; i < expected.size(); i++) {
      assertThat(actual.size())
        .describedAs("actual.size = " + actual.size() + ", expected.size = " + expected.size() + displayedLists)
        .isGreaterThan(i);
      assertThat(actual.get(i))
        .describedAs("Line " + (i + 1) + displayedLists)
        .isEqualTo(expected.get(i));
    }

    assertThat(actual).hasSameSizeAs(expected);

    assertThat(actualIsModified).isEqualTo(expectedModified);

    for (int i = 0; i < configLines.lines.size(); i++) {
      ConfigLine line = configLines.lines.get(i);
      if (line.command() != null) {
        assertThat(line.value())
          .describedAs("Line " + (i + 1) + displayedLists)
          .isNull();
      }

      if (line.value() != null) {
        assertThat(line.command())
          .describedAs("Line " + (i + 1) + displayedLists)
          .isNull();
      }
    }
  }

  private String printLines(List<String> list) {
    StringBuilder sb = new StringBuilder();
    int i = 1;
    for (String line : list) {
      sb.append("\nLINE ").append(i++).append(" : ").append(line);
    }
    return sb.toString();
  }

  private void put_testCore(List<String> startLines,
                            String key, String value, boolean expectedModified,
                            List<String> expected) {

    ConfigLines configLines = ConfigLines.fromBytes(linesToBytes(startLines), "wow/config.txt");

    //
    //
    configLines.put(key, value);
    boolean actualIsModified = configLines.isModified();
    //
    //

    List<String> actual = Arrays.asList(new String(configLines.toBytes(), UTF_8).split("\n"));

    String displayedLists = "\n\nActual lines:" + printLines(actual)
      + "\n\nExpected lines:" + printLines(expected) + "\n\n";

    for (int i = 0; i < configLines.lines.size(); i++) {
      ConfigLine line = configLines.lines.get(i);
      if (line.command() != null) {
        assertThat(line.value())
          .describedAs("Line " + (i + 1) + displayedLists)
          .isNull();
      }

      if (line.value() != null) {
        assertThat(line.command())
          .describedAs("Line " + (i + 1) + displayedLists)
          .isNull();
      }
    }

    for (int i = 0; i < expected.size(); i++) {
      assertThat(actual.get(i))
        .describedAs("Line " + (i + 1) + displayedLists)
        .isEqualTo(expected.get(i));
    }

    assertThat(actual).hasSameSizeAs(expected);

    assertThat(actualIsModified).isEqualTo(expectedModified);


  }

  @Test
  public void addValueVariant_1() {

    List<String> startLines = new ArrayList<>();
    startLines.add("  key001  = 123    ");
    startLines.add("# key002  = 321    ");
    startLines.add("  key002  = 334455 ");
    startLines.add("  key003  = oops   ");

    String key = "key002";
    String value = "321";

    List<String> expected = new ArrayList<>();
    expected.add("  key001  = 123    ");
    expected.add("# key002  = 321    ");
    expected.add("  key002  = 334455 ");
    expected.add("  key003  = oops   ");

    boolean expectedModified = false;

    addValueVariant_testCore(startLines, key, value, expectedModified, expected);

  }

  @Test
  public void addValueVariant_2() {

    List<String> startLines = new ArrayList<>();
    startLines.add("  key001  = 123    ");
    startLines.add("# key002  = 321    ");
    startLines.add("  key002  = 334455 ");
    startLines.add("  key003  = oops   ");

    String key = "key002";
    String value = "334455";

    List<String> expected = new ArrayList<>();
    expected.add("  key001  = 123    ");
    expected.add("# key002  = 321    ");
    expected.add("  key002  = 334455 ");
    expected.add("  key003  = oops   ");

    boolean expectedModified = false;

    addValueVariant_testCore(startLines, key, value, expectedModified, expected);

  }

  @Test
  public void addValueVariant_3() {

    List<String> startLines = new ArrayList<>();
    startLines.add("  key001  = 123    ");
    startLines.add("# key002  = 321    ");
    startLines.add("  key002  = 334455 ");
    startLines.add("  key003  = oops   ");

    String key = "key002";
    String value = "variant1";

    List<String> expected = new ArrayList<>();
    expected.add("  key001  = 123    ");
    expected.add("# key002  = 321    ");
    expected.add("  key002  = 334455 ");
    expected.add("# key002  = variant1");
    expected.add("  key003  = oops   ");

    boolean expectedModified = true;

    addValueVariant_testCore(startLines, key, value, expectedModified, expected);

  }

  @Test
  public void addValueVariant_4() {

    List<String> startLines = new ArrayList<>();
    startLines.add("  key001  = 123    ");
    startLines.add("# key002  = 321    ");
    startLines.add("  key002  = 334455 ");
    startLines.add("# key002  = variant1");
    startLines.add("  key003  = oops   ");

    String key = "key002";
    String value = "variant2";

    List<String> expected = new ArrayList<>();
    expected.add("  key001  = 123    ");
    expected.add("# key002  = 321    ");
    expected.add("  key002  = 334455 ");
    expected.add("# key002  = variant1");
    expected.add("# key002  = variant2");
    expected.add("  key003  = oops   ");

    boolean expectedModified = true;

    addValueVariant_testCore(startLines, key, value, expectedModified, expected);

  }

  @Test
  public void addValueVariant_5() {

    List<String> startLines = new ArrayList<>();
    startLines.add("  key001  = 123    ");
    startLines.add("# key002  = 321    ");
    startLines.add("  key002  = 334455 ");
    startLines.add("# key002  = variant1");
    startLines.add("# key002  = variant2");
    startLines.add("  key003  = oops   ");

    String key = "key002";
    String value = "x";

    List<String> expected = new ArrayList<>();
    expected.add("  key001  = 123    ");
    expected.add("# key002  = 321    ");
    expected.add("  key002  = 334455 ");
    expected.add("# key002  = variant1");
    expected.add("# key002  = variant2");
    expected.add("# key002  = x       ");
    expected.add("  key003  = oops   ");

    boolean expectedModified = true;

    addValueVariant_testCore(startLines, key, value, expectedModified, expected);

  }

  @Test
  public void addValueVariant_6() {

    List<String> startLines = new ArrayList<>();
    startLines.add("  key001  = 123    ");
    startLines.add("# key002  = 321    ");
    startLines.add("  key002  = 334455 ");
    startLines.add("# key002  = variant1");
    startLines.add("# key002  = variant2");
    startLines.add("  key003  = oops   ");

    String key = "left-key";
    String value = "x";

    List<String> expected = new ArrayList<>();
    expected.add("  key001  = 123    ");
    expected.add("# key002  = 321    ");
    expected.add("  key002  = 334455 ");
    expected.add("# key002  = variant1");
    expected.add("# key002  = variant2");
    expected.add("  key003  = oops   ");
    expected.add("# left-key= x      ");

    boolean expectedModified = true;

    addValueVariant_testCore(startLines, key, value, expectedModified, expected);

  }

  @Test
  public void addValueVariant_7() {

    List<String> startLines = new ArrayList<>();
    startLines.add("  key001  = 123    ");
    startLines.add("# key002  = 321    ");
    startLines.add("  key002  = 334455 ");
    startLines.add("# key002  = variant1");
    startLines.add("# key002  = variant2");
    startLines.add("  key003  = oops   ");

    String key = "x";
    String value = "y";

    List<String> expected = new ArrayList<>();
    expected.add("  key001  = 123    ");
    expected.add("# key002  = 321    ");
    expected.add("  key002  = 334455 ");
    expected.add("# key002  = variant1");
    expected.add("# key002  = variant2");
    expected.add("  key003  = oops   ");
    expected.add("# x       = y      ");

    boolean expectedModified = true;

    addValueVariant_testCore(startLines, key, value, expectedModified, expected);

  }

  @Test
  public void addValueVariant_8() {

    List<String> startLines = new ArrayList<>();
    startLines.add("  key001  = 123    ");
    startLines.add("# key002  = 321    ");
    startLines.add("  key002  = 334455 ");
    startLines.add("  key003  = oops   ");

    String key = "key002";
    String value = null;

    List<String> expected = new ArrayList<>();
    expected.add("  key001  = 123    ");
    expected.add("# key002  = 321    ");
    expected.add("  key002  = 334455 ");
    expected.add("# key002  : null");
    expected.add("  key003  = oops   ");

    boolean expectedModified = true;

    addValueVariant_testCore(startLines, key, value, expectedModified, expected);

  }

  @Test
  public void addValueVariant_9() {

    List<String> startLines = new ArrayList<>();
    startLines.add("  key001  = 123    ");
    startLines.add("# key002  = 321    ");
    startLines.add("  key002  = 334455 ");
    startLines.add("# key002  : null   ");
    startLines.add("  key003  = oops   ");

    String key = "key002";
    String value = "777";

    List<String> expected = new ArrayList<>();
    expected.add("  key001  = 123    ");
    expected.add("# key002  = 321    ");
    expected.add("  key002  = 334455 ");
    expected.add("# key002  : null   ");
    expected.add("# key002  = 777    ");
    expected.add("  key003  = oops   ");

    boolean expectedModified = true;

    addValueVariant_testCore(startLines, key, value, expectedModified, expected);

  }

  @Test
  public void put_1() {

    List<String> startLines = new ArrayList<>();
    startLines.add("  key001  = 123    ");
    startLines.add("# key002  = one    ");
    startLines.add("  key002  = two    ");
    startLines.add("# key002  = three  ");
    startLines.add("  key003  = oops   ");

    String key = "key002";
    String value = "one";

    List<String> expected = new ArrayList<>();
    expected.add("  key001  = 123    ");
    expected.add("  key002  = one    ");
    expected.add("# key002  = two    ");
    expected.add("# key002  = three  ");
    expected.add("  key003  = oops   ");

    boolean expectedModified = true;

    put_testCore(startLines, key, value, expectedModified, expected);

  }

  @Test
  public void put_2() {

    List<String> startLines = new ArrayList<>();
    startLines.add("  key001  = 123    ");
    startLines.add("# key002  = one    ");
    startLines.add("  key002  = two    ");
    startLines.add("# key002  = three  ");
    startLines.add("  key003  = oops   ");

    String key = "key002";
    String value = "two";

    List<String> expected = new ArrayList<>();
    expected.add("  key001  = 123    ");
    expected.add("# key002  = one    ");
    expected.add("  key002  = two    ");
    expected.add("# key002  = three  ");
    expected.add("  key003  = oops   ");

    boolean expectedModified = false;

    put_testCore(startLines, key, value, expectedModified, expected);

  }

  @Test
  public void put_3() {

    List<String> startLines = new ArrayList<>();
    startLines.add("  key001  = 123    ");
    startLines.add("# key002  = one    ");
    startLines.add("  key002  = two    ");
    startLines.add("# key002  = three  ");
    startLines.add("  key003  = oops   ");

    String key = "key002";
    String value = "three";

    List<String> expected = new ArrayList<>();
    expected.add("  key001  = 123    ");
    expected.add("# key002  = one    ");
    expected.add("# key002  = two    ");
    expected.add("  key002  = three  ");
    expected.add("  key003  = oops   ");

    boolean expectedModified = true;

    put_testCore(startLines, key, value, expectedModified, expected);

  }

  @Test
  public void put_4() {

    List<String> startLines = new ArrayList<>();
    startLines.add("  key001  = 123    ");
    startLines.add("# key002  = one    ");
    startLines.add("  key002  = two    ");
    startLines.add("# key002  = three  ");
    startLines.add("  key003  = oops   ");

    String key = "key002";
    String value = "four";

    List<String> expected = new ArrayList<>();
    expected.add("  key001  = 123    ");
    expected.add("# key002  = one    ");
    expected.add("# key002  = two    ");
    expected.add("# key002  = three  ");
    expected.add("  key002  = four   ");
    expected.add("  key003  = oops   ");

    boolean expectedModified = true;

    put_testCore(startLines, key, value, expectedModified, expected);

  }

  @Test
  public void put_5() {

    List<String> startLines = new ArrayList<>();
    startLines.add("  key001  = 123    ");
    startLines.add("# key002  = one    ");
    startLines.add("  key002  = two    ");
    startLines.add("# key002  = three  ");
    startLines.add("  key003  = oops   ");

    String key = "key002";
    String value = null;

    List<String> expected = new ArrayList<>();
    expected.add("  key001  = 123    ");
    expected.add("# key002  = one    ");
    expected.add("# key002  = two    ");
    expected.add("# key002  = three  ");
    expected.add("  key002  : null");
    expected.add("  key003  = oops   ");

    boolean expectedModified = true;

    put_testCore(startLines, key, value, expectedModified, expected);

  }

  @Test
  public void put_6() {

    List<String> startLines = new ArrayList<>();
    startLines.add("  key001  = 123    ");
    startLines.add("# key002  = one    ");
    startLines.add("# key002  = two    ");
    startLines.add("# key002  = three  ");
    startLines.add("  key002  : null   ");
    startLines.add("  key003  = oops   ");

    String key = "key002";
    String value = "two";

    List<String> expected = new ArrayList<>();
    expected.add("  key001  = 123    ");
    expected.add("# key002  = one    ");
    expected.add("  key002  = two    ");
    expected.add("# key002  = three  ");
    expected.add("# key002  : null   ");
    expected.add("  key003  = oops   ");

    boolean expectedModified = true;

    put_testCore(startLines, key, value, expectedModified, expected);

  }

  @Test
  public void put_7() {

    List<String> startLines = new ArrayList<>();
    startLines.add("  key001  = 123    ");
    startLines.add("# key002  = one    ");
    startLines.add("# key002  = two    ");
    startLines.add("# key002  = three  ");
    startLines.add("  key002  : null   ");
    startLines.add("  key003  = oops   ");

    String key = "key002";
    String value = "four";

    List<String> expected = new ArrayList<>();
    expected.add("  key001  = 123    ");
    expected.add("# key002  = one    ");
    expected.add("# key002  = two    ");
    expected.add("# key002  = three  ");
    expected.add("# key002  : null   ");
    expected.add("  key002  = four   ");
    expected.add("  key003  = oops   ");

    boolean expectedModified = true;

    put_testCore(startLines, key, value, expectedModified, expected);

  }

  @Test
  public void put_8() {

    List<String> startLines = new ArrayList<>();
    startLines.add("  key002  : null   ");
    startLines.add("  key003  = oops   ");

    String key = "key002";
    String value = "four";

    List<String> expected = new ArrayList<>();
    expected.add("# key002  : null   ");
    expected.add("  key002  = four   ");
    expected.add("  key003  = oops   ");

    boolean expectedModified = true;

    put_testCore(startLines, key, value, expectedModified, expected);

  }
}
