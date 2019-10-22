package kz.greetgo.kafka.util;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public class GenericUtil {
  public static boolean isOfClass(Type someType, Class<?> aClass) {
    if (someType == aClass) {
      return true;
    }

    if (someType instanceof ParameterizedType) {
      ParameterizedType type = (ParameterizedType) someType;
      //noinspection RedundantIfStatement
      if (type.getRawType() == aClass) {
        return true;
      }
    }

    return false;
  }

  public static Class<?> extractClass(Type someType) {
    if (someType instanceof Class) {
      return (Class<?>) someType;
    }

    if (someType instanceof ParameterizedType) {
      ParameterizedType type = (ParameterizedType) someType;
      if (type.getRawType() instanceof Class) {
        return (Class<?>) type.getRawType();
      }
    }

    throw new IllegalArgumentException("Cannot extract class from " + someType);
  }

  public static long longNullAsZero(Long value) {
    return value == null ? 0L : value;
  }
}
