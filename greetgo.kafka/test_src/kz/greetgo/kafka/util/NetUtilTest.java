package kz.greetgo.kafka.util;

import org.testng.annotations.Test;

import static org.fest.assertions.api.Assertions.assertThat;

public class NetUtilTest {

  @Test
  public void canConnectTo_ok() {

    //
    //
    boolean hasConnection = NetUtil.canConnectTo("localhost:11223");
    //
    //

    assertThat(hasConnection).isFalse();

  }
}
