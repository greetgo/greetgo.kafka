package kz.greetgo.kafka2.util;

import org.testng.annotations.Test;

import java.util.Random;

import static kz.greetgo.kafka2.util.HexUtil.bytesToHex;
import static kz.greetgo.kafka2.util.HexUtil.hexToBytes;
import static org.fest.assertions.api.Assertions.assertThat;

public class HexUtilTest {

  @Test
  public void bytesToHex_hexToBytes() {

    Random random = new Random();
    byte[] bytes = new byte[random.nextInt(10) + 7];
    random.nextBytes(bytes);

    String hex = bytesToHex(bytes);

    System.out.println(hex);

    byte[] actualBytes = hexToBytes(hex);

    assertThat(actualBytes).isEqualTo(bytes);
  }

  @Test
  public void bytesToHex_hexToBytes_1() {

    byte[] bytes = new byte[]{1, 2, 3, 4};

    String hex = bytesToHex(bytes);

    System.out.println(hex);

    byte[] actualBytes = hexToBytes(hex);

    assertThat(actualBytes).isEqualTo(bytes);
  }

  @Test
  public void bytesToHex_hexToBytes_2() {

    byte[] bytes = new byte[]{101, 102, 103, 104};

    String hex = bytesToHex(bytes);

    System.out.println(hex);

    byte[] actualBytes = hexToBytes(hex);

    assertThat(actualBytes).isEqualTo(bytes);
  }

  @Test
  public void bytesToHex_1() {

    byte[] bytes = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

    String hex = bytesToHex(bytes);

    assertThat(hex).isEqualTo("000102030405060708090A0B0C0D0E0F");
  }

  @Test
  public void bytesToHex_2() {

    byte[] bytes = {16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31};

    String hex = bytesToHex(bytes);

    assertThat(hex).isEqualTo("101112131415161718191A1B1C1D1E1F");
  }

  @Test
  public void bytesToHex_3() {

    byte[] bytes = {112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127};

    String hex = bytesToHex(bytes);

    assertThat(hex).isEqualTo("707172737475767778797A7B7C7D7E7F");
  }

  @Test
  public void bytesToHex_4() {

    byte[] bytes = {-128, -127, -126, -125, -124, -123, -122, -121, -120, -119, -118, -117, -116, -115, -114, -113};

    String hex = bytesToHex(bytes);

    assertThat(hex).isEqualTo("808182838485868788898A8B8C8D8E8F");
  }


  @Test
  public void bytesToHex_5() {

    byte[] bytes = {-16, -15, -14, -13, -12, -11, -10, -9, -8, -7, -6, -5, -4, -3, -2, -1};

    String hex = bytesToHex(bytes);

    //noinspection SpellCheckingInspection
    assertThat(hex).isEqualTo("F0F1F2F3F4F5F6F7F8F9FAFBFCFDFEFF");
  }


  @Test
  public void hexToBytes_5() {

    //noinspection SpellCheckingInspection
    String hex = "F0F1F2F3F4F5F6F7F8F9FAFBFCFDFEFF";

    byte[] expected = {-16, -15, -14, -13, -12, -11, -10, -9, -8, -7, -6, -5, -4, -3, -2, -1};

    byte[] actual = hexToBytes(hex);

    assertThat(actual).isEqualTo(expected);
  }


  @Test
  public void hexToBytes_51() {

    String hex = "FF";

    byte[] expected = {-1};

    byte[] actual = hexToBytes(hex);

    assertThat(actual).isEqualTo(expected);
  }
}
