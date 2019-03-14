package kz.greetgo.kafka.util;

import org.testng.annotations.Test;

import static org.fest.assertions.api.Assertions.assertThat;

public class ConfigLineValueTest {

  @Test
  public void parse_value() {

    //
    //
    ConfigLineValue clv = ConfigLineValue.parse(" sinus =   hello world     ");
    //
    //

    assertThat(clv.command()).isNull();
    assertThat(clv.value()).isEqualTo("hello world");
    assertThat(clv.paddingLeft()).isEqualTo(3);
    assertThat(clv.width()).isEqualTo(16);
    assertThat(clv.toString()).isEqualTo("=   hello world     ");
    assertThat(clv.errors()).isEmpty();

  }

  @Test
  public void parse_value_noAnySpaces() {

    //
    //
    ConfigLineValue clv = ConfigLineValue.parse("#key76=hello");
    //
    //

    assertThat(clv.command()).isNull();
    assertThat(clv.value()).isEqualTo("hello");
    assertThat(clv.paddingLeft()).isEqualTo(0);
    assertThat(clv.width()).isEqualTo(5);
    assertThat(clv.toString()).isEqualTo("=hello");
    assertThat(clv.errors()).isEmpty();

  }

  @Test
  public void parse_command_null() {

    //
    //
    ConfigLineValue clv = ConfigLineValue.parse(" sinus :   null     ");
    //
    //

    assertThat(clv.command()).isEqualTo(ConfigLineCommand.NULL);
    assertThat(clv.value()).isNull();
    assertThat(clv.paddingLeft()).isEqualTo(3);
    assertThat(clv.width()).isEqualTo(9);
    assertThat(clv.toString()).isEqualTo(":   null     ");
    assertThat(clv.errors()).isEmpty();

  }


  @Test
  public void parse_command_inherits() {

    //
    //
    ConfigLineValue clv = ConfigLineValue.parse(" sinus :     inherits     ");
    //
    //

    assertThat(clv.command()).isEqualTo(ConfigLineCommand.INHERITS);
    assertThat(clv.value()).isNull();
    assertThat(clv.paddingLeft()).isEqualTo(5);
    assertThat(clv.width()).isEqualTo(13);
    assertThat(clv.toString()).isEqualTo(":     inherits     ");
    assertThat(clv.errors()).isEmpty();

  }


  @Test
  public void parse_error() {

    //
    //
    ConfigLineValue clv = ConfigLineValue.parse(" sinus :   hello world     ");
    //
    //

    assertThat(clv.command()).isEqualTo(ConfigLineCommand.UNKNOWN);
    assertThat(clv.value()).isNull();
    assertThat(clv.paddingLeft()).isEqualTo(3);
    assertThat(clv.width()).isEqualTo(16);
    assertThat(clv.toString()).isEqualTo(":   hello world     ");
    assertThat(clv.errors()).isNotEmpty();
    assertThat(clv.errors().get(0)).contains("hello world");

  }

  @Test
  public void parse_returnsNull() {

    //
    //
    ConfigLineValue clv = ConfigLineValue.parse(" sinus hello world     ");
    //
    //

    assertThat(clv).isNull();

  }


  @SuppressWarnings("ConstantConditions")
  @Test
  public void parse_nullReturnsNull() {

    //
    //
    ConfigLineValue clv = ConfigLineValue.parse(null);
    //
    //

    assertThat(clv).isNull();

  }


  @Test
  public void parse_returnsNull_sharps() {

    //
    //
    ConfigLineValue clv = ConfigLineValue.parse("   ##  sinus hello : world     ");
    //
    //

    assertThat(clv).isNull();

  }


  @Test
  public void setValue_longer() {
    ConfigLineValue clv = ConfigLineValue.parse(" sinus =   hello world     ");

    //
    //
    clv.setValue("Переход на светлую сторону Силы");
    //
    //

    assertThat(clv.command()).isNull();
    assertThat(clv.value()).isEqualTo("Переход на светлую сторону Силы");
    assertThat(clv.paddingLeft()).isEqualTo(3);
    assertThat(clv.width()).isEqualTo(31);
    assertThat(clv.toString()).isEqualTo("=   Переход на светлую сторону Силы");
    assertThat(clv.errors()).isEmpty();
  }

  @Test
  public void setValue_shorter() {
    ConfigLineValue clv = ConfigLineValue.parse(" sinus =   hello world     ");

    //
    //
    clv.setValue("hi");
    //
    //

    assertThat(clv.command()).isNull();
    assertThat(clv.value()).isEqualTo("hi");
    assertThat(clv.paddingLeft()).isEqualTo(3);
    assertThat(clv.width()).isEqualTo(16);
    assertThat(clv.toString()).isEqualTo("=   hi              ");
    assertThat(clv.errors()).isEmpty();
  }

  @Test
  public void setValue_null() {
    ConfigLineValue clv = ConfigLineValue.parse(" sinus =   hello world     ");

    //
    //
    clv.setValue(null);
    //
    //

    assertThat(clv.command()).isEqualTo(ConfigLineCommand.NULL);
    assertThat(clv.value()).isNull();
    assertThat(clv.paddingLeft()).isEqualTo(3);
    assertThat(clv.width()).isEqualTo(16);
    assertThat(clv.toString()).isEqualTo(":   null            ");
    assertThat(clv.errors()).isEmpty();
  }

  @Test
  public void setCommand_NULL() {
    ConfigLineValue clv = ConfigLineValue.parse(" sinus =   hello world     ");

    //
    //
    clv.setCommand(ConfigLineCommand.NULL);
    //
    //

    assertThat(clv.command()).isEqualTo(ConfigLineCommand.NULL);
    assertThat(clv.value()).isNull();
    assertThat(clv.paddingLeft()).isEqualTo(3);
    assertThat(clv.width()).isEqualTo(16);
    assertThat(clv.toString()).isEqualTo(":   null            ");
    assertThat(clv.errors()).isEmpty();
  }

  @Test
  public void setCommand_UNKNOWN() {
    ConfigLineValue clv = ConfigLineValue.parse(" sinus =   hello world     ");

    //
    //
    clv.setCommand(ConfigLineCommand.UNKNOWN);
    //
    //

    assertThat(clv.command()).isEqualTo(ConfigLineCommand.UNKNOWN);
    assertThat(clv.value()).isNull();
    assertThat(clv.paddingLeft()).isEqualTo(3);
    assertThat(clv.width()).isEqualTo(16);
    assertThat(clv.toString()).isEqualTo(":   unknown         ");
    assertThat(clv.errors()).isNotEmpty();
  }


  @Test
  public void setCommand_INHERITS() {
    ConfigLineValue clv = ConfigLineValue.parse(" sinus =   hello world     ");

    //
    //
    clv.setCommand(ConfigLineCommand.INHERITS);
    //
    //

    assertThat(clv.command()).isEqualTo(ConfigLineCommand.INHERITS);
    assertThat(clv.value()).isNull();
    assertThat(clv.paddingLeft()).isEqualTo(3);
    assertThat(clv.width()).isEqualTo(16);
    assertThat(clv.toString()).isEqualTo(":   inherits        ");
    assertThat(clv.errors()).isEmpty();
  }
}
