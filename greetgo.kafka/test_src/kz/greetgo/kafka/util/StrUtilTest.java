package kz.greetgo.kafka.util;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static kz.greetgo.kafka.util.StrUtil.firstIndexOf;
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

  @Test
  public void intToStrLen_1() {

    //
    //
    String str = StrUtil.intToStrLen(13, 4);
    //
    //

    assertThat(str).isEqualTo("0013");
  }


  @DataProvider
  public Object[][] extractParentPathDataProvider() {
    return new Object[][]{
      {"qwe/ewq", "qwe"},
      {"qwe/wow/123/www", "qwe/wow/123"},
      {"/qwe/ewq", "/qwe"},
      {"/qwe/wow/123/www", "/qwe/wow/123"},
      {"/qwe/wow/123/////www", "/qwe/wow/123"},
      {"/qwe", "/"},
      {"qwe", null},
      {"/", null},
      {"/////", null},
      {"hello/status/xxx/", "hello/status"},
      {"hello/status/xxx/////", "hello/status"},

      {null, null},
    };
  }

  @Test(dataProvider = "extractParentPathDataProvider")
  public void extractParentPath(String path, String parentPath) {

    //
    //
    String actualParentPath = StrUtil.extractParentPath(path);
    //
    //

    assertThat(actualParentPath).isEqualTo(parentPath);

  }
}
