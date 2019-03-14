package kz.greetgo.kafka2.util;

import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.List;

import static kz.greetgo.kafka2.util.GenericUtil.isOfClass;
import static kz.greetgo.kafka2.util_for_tests.ReflectionUtil.findMethod;
import static org.fest.assertions.api.Assertions.assertThat;

public class GenericUtilTest {

  static class Class_isOfClass {

    @SuppressWarnings("unused")
    public void method1(int par1, List<String> par2) {}

  }

  @Test
  public void isOfClass_ok() {
    Method method = findMethod(new Class_isOfClass(), "method1");

    Type[] types = method.getGenericParameterTypes();

    assertThat(isOfClass(types[0], Integer.class)).isFalse();
    assertThat(isOfClass(types[0], int.class)).isTrue();

    assertThat(isOfClass(types[1], List.class)).isTrue();

  }
}
