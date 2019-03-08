package kz.greetgo.kafka2.util;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static kz.greetgo.kafka2.util.StrUtil.firstIndexOf;
import static org.fest.assertions.api.Assertions.assertThat;

public class StrUtilTest {

  @DataProvider
  public Object[][] firstIndexOf_DataProvider() {
    return new Object[][]{
        {"sinus = pool : hello", 6},
        {"status = hello", 7},
        {"# stone : sinus = hello", 8},
        {"# stone : sinus", 8},
        {"# oops", -1},
        {null, -1},
    };
  }

  @Test(dataProvider = "firstIndexOf_DataProvider")
  public void firstIndexOf_ok(String str, int expectedIndex) {

    int index = firstIndexOf(str, '=', ':');

    assertThat(index).isEqualTo(expectedIndex);

  }
}
