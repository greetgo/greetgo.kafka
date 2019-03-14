package kz.greetgo.kafka.util;

import org.testng.annotations.Test;

import static org.fest.assertions.api.Assertions.assertThat;

public class ConfigLineTest {

  @Test
  public void parse_normal_value() {

    ConfigLine line = ConfigLine.parse("  key01  =  valueONE   ");

    assertThat(line.key()).isEqualTo("key01");
    assertThat(line.keyPart()).isEqualTo("  key01  ");

    assertThat(line.value()).isEqualTo("valueONE");
    assertThat(line.valuePart()).isEqualTo("=  valueONE   ");
    assertThat(line.command()).isNull();

    assertThat(line.line()).isEqualTo("  key01  =  valueONE   ");
    assertThat(line.isCommented()).isFalse();
    assertThat(line.errors()).isEmpty();
  }

  @Test
  public void parse_commandInherits() {

    ConfigLine line = ConfigLine.parse("  key01  :  inherits   ");

    assertThat(line.key()).isEqualTo("key01");
    assertThat(line.keyPart()).isEqualTo("  key01  ");

    assertThat(line.value()).isNull();
    assertThat(line.valuePart()).isEqualTo(":  inherits   ");
    assertThat(line.command()).isEqualTo(ConfigLineCommand.INHERITS);

    assertThat(line.line()).isEqualTo("  key01  :  inherits   ");
    assertThat(line.isCommented()).isFalse();
    assertThat(line.errors()).isEmpty();

  }

  @Test
  public void parse_commandNull() {

    ConfigLine line = ConfigLine.parse("  key01  :  null   ");

    assertThat(line.key()).isEqualTo("key01");
    assertThat(line.keyPart()).isEqualTo("  key01  ");

    assertThat(line.value()).isNull();
    assertThat(line.valuePart()).isEqualTo(":  null   ");
    assertThat(line.command()).isEqualTo(ConfigLineCommand.NULL);

    assertThat(line.line()).isEqualTo("  key01  :  null   ");
    assertThat(line.isCommented()).isFalse();
    assertThat(line.errors()).isEmpty();

  }

  @Test
  public void parse_normal_value_COMMENTED() {

    ConfigLine line = ConfigLine.parse("  #  key01  =  valueONE   ");

    assertThat(line.key()).isEqualTo("key01");
    assertThat(line.keyPart()).isEqualTo("  #  key01  ");

    assertThat(line.value()).isEqualTo("valueONE");
    assertThat(line.valuePart()).isEqualTo("=  valueONE   ");
    assertThat(line.command()).isNull();

    assertThat(line.line()).isEqualTo("  #  key01  =  valueONE   ");
    assertThat(line.isCommented()).isTrue();
    assertThat(line.errors()).isEmpty();
  }

  @Test
  public void parse_commandInherits_COMMENTED() {

    ConfigLine line = ConfigLine.parse("  #  key01  :  inherits   ");

    assertThat(line.key()).isEqualTo("key01");
    assertThat(line.keyPart()).isEqualTo("  #  key01  ");

    assertThat(line.value()).isNull();
    assertThat(line.valuePart()).isEqualTo(":  inherits   ");
    assertThat(line.command()).isEqualTo(ConfigLineCommand.INHERITS);

    assertThat(line.line()).isEqualTo("  #  key01  :  inherits   ");
    assertThat(line.isCommented()).isTrue();
    assertThat(line.errors()).isEmpty();

  }

  @Test
  public void parse_commandNull_COMMENTED() {

    ConfigLine line = ConfigLine.parse("  #  key01  :  null   ");

    assertThat(line.key()).isEqualTo("key01");
    assertThat(line.keyPart()).isEqualTo("  #  key01  ");

    assertThat(line.value()).isNull();
    assertThat(line.valuePart()).isEqualTo(":  null   ");
    assertThat(line.command()).isEqualTo(ConfigLineCommand.NULL);

    assertThat(line.line()).isEqualTo("  #  key01  :  null   ");
    assertThat(line.isCommented()).isTrue();
    assertThat(line.errors()).isEmpty();

  }

  @Test
  public void setValue_withSpaces() {

    ConfigLine line = ConfigLine.parse("   #    key07  =    hello   ");

    assertThat(line.value()).isEqualTo("hello");
    assertThat(line.valuePart()).isEqualTo("=    hello   ");

    line.setValue("cornet");

    assertThat(line.value()).isEqualTo("cornet");
    assertThat(line.valuePart()).isEqualTo("=    cornet  ");

    line.setValue("c");

    assertThat(line.value()).isEqualTo("c");
    assertThat(line.valuePart()).isEqualTo("=    c       ");
  }

  @Test
  public void setValue_noSpaces() {

    ConfigLine line = ConfigLine.parse("#key07=hello");

    assertThat(line.value()).isEqualTo("hello");
    assertThat(line.valuePart()).isEqualTo("=hello");

    line.setValue("cornet");

    assertThat(line.value()).isEqualTo("cornet");
    assertThat(line.valuePart()).isEqualTo("=cornet");

    line.setValue("c");

    assertThat(line.value()).isEqualTo("c");
    assertThat(line.valuePart()).isEqualTo("=c     ");
  }

  @Test
  public void setCommented_withSpaces() {

    ConfigLine line = ConfigLine.parse("   #    key76  =    hello   ");

    assertThat(line.key()).isEqualTo("key76");
    assertThat(line.keyPart()).isEqualTo("   #    key76  ");

    line.setCommented(false);

    assertThat(line.key()).isEqualTo("key76");
    assertThat(line.keyPart()).isEqualTo("        key76  ");

    line.setCommented(true);

    assertThat(line.key()).isEqualTo("key76");
    assertThat(line.keyPart()).isEqualTo("   #    key76  ");
  }

  @Test
  public void setCommented_withoutSpaces() {

    ConfigLine line = ConfigLine.parse("#key76=hello");

    assertThat(line.key()).isEqualTo("key76");
    assertThat(line.keyPart()).isEqualTo("#key76");

    line.setCommented(false);

    assertThat(line.key()).isEqualTo("key76");
    assertThat(line.keyPart()).isEqualTo(" key76");

    line.setCommented(true);

    assertThat(line.key()).isEqualTo("key76");
    assertThat(line.keyPart()).isEqualTo("#key76");
  }

  @Test
  public void setCommented_withoutSpaces2() {

    ConfigLine line = ConfigLine.parse("key76=hello");

    assertThat(line.key()).isEqualTo("key76");
    assertThat(line.keyPart()).isEqualTo("key76");

    line.setCommented(true);

    assertThat(line.key()).isEqualTo("key76");
    assertThat(line.keyPart()).isEqualTo("#key76");

    line.setCommented(false);

    assertThat(line.key()).isEqualTo("key76");
    assertThat(line.keyPart()).isEqualTo(" key76");
  }

  @Test
  public void setCommand_commented_NULL() {
    ConfigLine line = ConfigLine.parse("#key76=hello");

    //
    //
    line.setCommand(ConfigLineCommand.NULL);
    //
    //

    assertThat(line.line()).isEqualTo("#key76:null ");
    assertThat(line.value()).isNull();
    assertThat(line.command()).isEqualTo(ConfigLineCommand.NULL);
    assertThat(line.errors()).isEmpty();

  }

  @Test
  public void setCommand_withSpaces_NULL() {
    ConfigLine line = ConfigLine.parse(" # key80 =   hello ");

    //
    //
    line.setCommand(ConfigLineCommand.NULL);
    //
    //

    assertThat(line.line()).isEqualTo(" # key80 :   null  ");
    assertThat(line.value()).isNull();
    assertThat(line.command()).isEqualTo(ConfigLineCommand.NULL);
    assertThat(line.errors()).isEmpty();

  }

  @Test
  public void setCommand_withSpaces_INHERITS() {
    ConfigLine line = ConfigLine.parse(" # key80 =   hello ");

    //
    //
    line.setCommand(ConfigLineCommand.INHERITS);
    //
    //

    assertThat(line.line()).isEqualTo(" # key80 :   inherits");
    assertThat(line.value()).isNull();
    assertThat(line.command()).isEqualTo(ConfigLineCommand.INHERITS);
    assertThat(line.errors()).isEmpty();

  }

  @Test
  public void setCommand_withSpaces_UNKNOWN() {
    ConfigLine line = ConfigLine.parse(" # key80 =   hello ");

    //
    //
    line.setCommand(ConfigLineCommand.UNKNOWN);
    //
    //

    assertThat(line.line()).isEqualTo(" # key80 :   unknown");
    assertThat(line.value()).isNull();
    assertThat(line.command()).isEqualTo(ConfigLineCommand.UNKNOWN);
    assertThat(line.errors()).isNotEmpty();

  }

  @Test
  public void parse_unknownCommand() {
    ConfigLine line = ConfigLine.parse(" # key80 : left_command ");

    assertThat(line.line()).isEqualTo(" # key80 : left_command ");
    assertThat(line.value()).isNull();
    assertThat(line.command()).isEqualTo(ConfigLineCommand.UNKNOWN);
    assertThat(line.errors()).isNotEmpty();

  }

  @Test
  public void parse_unknownLineFormat() {
    ConfigLine line = ConfigLine.parse(" hello world ");

    assertThat(line.line()).isEqualTo(" hello world ");
    assertThat(line.value()).isNull();
    assertThat(line.key()).isNull();
    assertThat(line.command()).isNull();
    assertThat(line.isCommented()).isTrue();
    assertThat(line.errors()).isNotEmpty();
  }

  @Test
  public void parse_unknownLineFormat_commented() {
    ConfigLine line = ConfigLine.parse(" # hello world ");

    assertThat(line.line()).isEqualTo(" # hello world ");
    assertThat(line.value()).isNull();
    assertThat(line.key()).isNull();
    assertThat(line.command()).isNull();
    assertThat(line.isCommented()).isTrue();
    assertThat(line.errors()).isNotEmpty();
  }

  @Test
  public void parse_doubleCommented() {
    ConfigLine line = ConfigLine.parse(" ## hello world ");

    assertThat(line.line()).isEqualTo(" ## hello world ");
    assertThat(line.value()).isNull();
    assertThat(line.key()).isNull();
    assertThat(line.command()).isNull();
    assertThat(line.isCommented()).isTrue();
    assertThat(line.errors()).isEmpty();
  }

  @Test
  public void parse_isEmpty() {
    ConfigLine line = ConfigLine.parse("    ");

    assertThat(line.line()).isEqualTo("    ");
    assertThat(line.value()).isNull();
    assertThat(line.key()).isNull();
    assertThat(line.command()).isNull();
    assertThat(line.isCommented()).isTrue();
    assertThat(line.errors()).isEmpty();
  }

  @Test
  public void setKey_withSpaces_commented() {

    ConfigLine line = ConfigLine.parse("  #   c3p0      =   line size wow   ");

    //
    //
    line.setKey("station");
    //
    //

    assertThat(line.line()).isEqualTo("  #   station   =   line size wow   ");
  }

  @Test
  public void setKey_withSpaces() {

    ConfigLine line = ConfigLine.parse("    c3p0      =   line size wow   ");

    //
    //
    line.setKey("station");
    //
    //

    assertThat(line.line()).isEqualTo("    station   =   line size wow   ");
  }

  @Test
  public void setKey_noSpaces() {

    ConfigLine line = ConfigLine.parse("c3p0=line size wow");

    //
    //
    line.setKey("station");
    //
    //

    assertThat(line.line()).isEqualTo("station=line size wow");
  }

  @Test
  public void setKey_commented() {

    ConfigLine line = ConfigLine.parse("## comment");

    //
    //
    line.setKey("station");
    //
    //

    assertThat(line.line()).isEqualTo("#station : null");
  }
}
