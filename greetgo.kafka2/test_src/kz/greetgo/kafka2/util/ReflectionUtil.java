package kz.greetgo.kafka2.util;

import java.lang.reflect.Method;

public class ReflectionUtil {
  public static Method findMethod(Object object, String methodName) {
    for (Method method : object.getClass().getMethods()) {
      if (method.getName().equals(methodName)) {
        return method;
      }
    }
    throw new RuntimeException("Cannot find method " + methodName + " in " + object.getClass());
  }
}
