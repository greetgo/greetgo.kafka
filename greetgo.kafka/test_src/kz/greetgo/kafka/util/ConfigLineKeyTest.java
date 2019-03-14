package kz.greetgo.kafka.util;

import org.testng.annotations.Test;

import static org.fest.assertions.api.Assertions.assertThat;

public class ConfigLineKeyTest {

  @SuppressWarnings("ConstantConditions")
  @Test
  public void parse_nullReturnsNull() {

    //
    //
    ConfigLineKey key = ConfigLineKey.parse(null);
    //
    //

    assertThat(key).isNull();


  }

  @Test
  public void parse_doubleSharpReturnsNull() {

    //
    //
    ConfigLineKey key = ConfigLineKey.parse("  ## h5h564 h5b46hb54 jn765n75 ");
    //
    //

    assertThat(key).isNull();

  }

  @Test
  public void parse_noValuePart() {

    //
    //
    ConfigLineKey key = ConfigLineKey.parse("  h5h564 h5b46hb54 jn765n75 ");
    //
    //

    assertThat(key).isNull();

  }


  @Test
  public void parse_value_noComment() {

    //
    //
    ConfigLineKey key = ConfigLineKey.parse("    hello   = some value ");
    //
    //

    assertThat(key).isNotNull();
    assert key != null;
    assertThat(key.isCommented()).isFalse();
    assertThat(key.key()).isEqualTo("hello");
    assertThat(key.paddingLeft1()).isEqualTo(0);
    assertThat(key.paddingLeft2()).isEqualTo(4);
    assertThat(key.width()).isEqualTo(8);
    assertThat(key.toString()).isEqualTo("    hello   ");

  }

  @Test
  public void parse_command_noComment() {

    //
    //
    ConfigLineKey key = ConfigLineKey.parse("   status     : some command ");
    //
    //

    assertThat(key).isNotNull();
    assert key != null;
    assertThat(key.isCommented()).isFalse();
    assertThat(key.key()).isEqualTo("status");
    assertThat(key.paddingLeft1()).isEqualTo(0);
    assertThat(key.paddingLeft2()).isEqualTo(3);
    assertThat(key.width()).isEqualTo(11);
    assertThat(key.toString()).isEqualTo("   status     ");
  }


  @Test
  public void parse_command_noComment_noAnyPadding() {

    //
    //
    ConfigLineKey key = ConfigLineKey.parse("status: some command ");
    //
    //

    assertThat(key).isNotNull();
    assert key != null;
    assertThat(key.isCommented()).isFalse();
    assertThat(key.key()).isEqualTo("status");
    assertThat(key.paddingLeft1()).isEqualTo(0);
    assertThat(key.paddingLeft2()).isEqualTo(0);
    assertThat(key.width()).isEqualTo(6);
    assertThat(key.toString()).isEqualTo("status");
  }

  @Test
  public void parse_value_comment() {

    //
    //
    ConfigLineKey key = ConfigLineKey.parse("  #  hello   = some value ");
    //
    //

    assertThat(key).isNotNull();
    assert key != null;
    assertThat(key.isCommented()).isTrue();
    assertThat(key.key()).isEqualTo("hello");
    assertThat(key.paddingLeft1()).isEqualTo(2);
    assertThat(key.paddingLeft2()).isEqualTo(3);
    assertThat(key.width()).isEqualTo(8);
    assertThat(key.toString()).isEqualTo("  #  hello   ");
  }

  @Test
  public void parse_command_comment() {

    //
    //
    ConfigLineKey key = ConfigLineKey.parse("   #     status        : some command ");
    //
    //


    assertThat(key).isNotNull();
    assert key != null;
    assertThat(key.isCommented()).isTrue();
    assertThat(key.key()).isEqualTo("status");
    assertThat(key.paddingLeft1()).isEqualTo(3);
    assertThat(key.paddingLeft2()).isEqualTo(6);
    assertThat(key.width()).isEqualTo(14);
    assertThat(key.toString()).isEqualTo("   #     status        ");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void setKey_null_IllegalArgumentException() {

    ConfigLineKey key = ConfigLineKey.parse("   #     status        : some command ");

    assert key != null;

    //
    //
    key.setKey(null);
    //
    //
  }

  @Test
  public void setKey_longer_commented() {

    ConfigLineKey key = ConfigLineKey.parse("   #     status        : some command ");

    assert key != null;

    //
    //
    key.setKey("Начинается новый день в городе Алма-Ате");
    //
    //

    assertThat(key.isCommented()).isTrue();
    assertThat(key.key()).isEqualTo("Начинается новый день в городе Алма-Ате");
    assertThat(key.paddingLeft1()).isEqualTo(3);
    assertThat(key.paddingLeft2()).isEqualTo(6);
    assertThat(key.width()).isEqualTo(39);
    assertThat(key.toString()).isEqualTo("   #     Начинается новый день в городе Алма-Ате");
  }

  @Test
  public void setKey_longer_notCommented() {

    ConfigLineKey key = ConfigLineKey.parse("   sinus      : some command ");

    assert key != null;

    //
    //
    key.setKey("Начинается новый день в городе Алма-Ате");
    //
    //

    assertThat(key.isCommented()).isFalse();
    assertThat(key.key()).isEqualTo("Начинается новый день в городе Алма-Ате");
    assertThat(key.paddingLeft1()).isEqualTo(0);
    assertThat(key.paddingLeft2()).isEqualTo(3);
    assertThat(key.width()).isEqualTo(39);
    assertThat(key.toString()).isEqualTo("   Начинается новый день в городе Алма-Ате");
  }

  @Test
  public void setKey_shorter_commented() {

    ConfigLineKey key = ConfigLineKey.parse("    #    status        : some command ");

    assert key != null;

    //
    //
    key.setKey("x");
    //
    //

    assertThat(key.isCommented()).isTrue();
    assertThat(key.key()).isEqualTo("x");
    assertThat(key.paddingLeft1()).isEqualTo(4);
    assertThat(key.paddingLeft2()).isEqualTo(5);
    assertThat(key.width()).isEqualTo(14);
    assertThat(key.toString()).isEqualTo("    #    x             ");
  }

  @Test
  public void setKey_shorter_notCommented() {

    ConfigLineKey key = ConfigLineKey.parse("     transaction      : some command ");

    assert key != null;

    //
    //
    key.setKey("oops");
    //
    //

    assertThat(key.isCommented()).isFalse();
    assertThat(key.key()).isEqualTo("oops");
    assertThat(key.paddingLeft1()).isEqualTo(0);
    assertThat(key.paddingLeft2()).isEqualTo(5);
    assertThat(key.width()).isEqualTo(17);
    assertThat(key.toString()).isEqualTo("     oops             ");
  }

  @Test
  public void setCommented_true_notCommented() {

    ConfigLineKey key = ConfigLineKey.parse("       transaction      : some command ");

    assert key != null;

    //
    //
    key.setCommented(true);
    //
    //

    assertThat(key.isCommented()).isTrue();
    assertThat(key.key()).isEqualTo("transaction");
    assertThat(key.paddingLeft1()).isEqualTo(0);
    assertThat(key.paddingLeft2()).isEqualTo(7);
    assertThat(key.width()).isEqualTo(17);
    assertThat(key.toString()).isEqualTo("#      transaction      ");
  }

  @Test
  public void setCommented_true_notCommented_noAnyPadding() {

    ConfigLineKey key = ConfigLineKey.parse("transaction:  some command ");

    assert key != null;

    //
    //
    key.setCommented(true);
    //
    //

    assertThat(key.isCommented()).isTrue();
    assertThat(key.key()).isEqualTo("transaction");
    assertThat(key.paddingLeft1()).isEqualTo(0);
    assertThat(key.paddingLeft2()).isEqualTo(1);
    assertThat(key.width()).isEqualTo(11);
    assertThat(key.toString()).isEqualTo("#transaction");
  }

  @Test
  public void setCommented_false_commented() {

    ConfigLineKey key = ConfigLineKey.parse("     #  transaction        = hi");

    assert key != null;

    //
    //
    key.setCommented(false);
    //
    //

    assertThat(key.isCommented()).isFalse();
    assertThat(key.key()).isEqualTo("transaction");
    assertThat(key.paddingLeft1()).isEqualTo(5);
    assertThat(key.paddingLeft2()).isEqualTo(3);
    assertThat(key.width()).isEqualTo(19);
    assertThat(key.toString()).isEqualTo("        transaction        ");
  }

  @Test
  public void setCommented_falseThenTrue_commented() {

    ConfigLineKey key = ConfigLineKey.parse("     #  transaction        = hi");

    assert key != null;

    //
    //
    key.setCommented(false);
    //
    //

    assertThat(key.isCommented()).isFalse();
    assertThat(key.key()).isEqualTo("transaction");
    assertThat(key.paddingLeft1()).isEqualTo(5);
    assertThat(key.paddingLeft2()).isEqualTo(3);
    assertThat(key.width()).isEqualTo(19);
    assertThat(key.toString()).isEqualTo("        transaction        ");

    //
    //
    key.setCommented(true);
    //
    //

    assertThat(key.isCommented()).isTrue();
    assertThat(key.key()).isEqualTo("transaction");
    assertThat(key.paddingLeft1()).isEqualTo(5);
    assertThat(key.paddingLeft2()).isEqualTo(3);
    assertThat(key.width()).isEqualTo(19);
    assertThat(key.toString()).isEqualTo("     #  transaction        ");
  }
}
