package kz.greetgo.kafka.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

public class AnnotationUtil {

  public static <T extends Annotation> T getAnnotation(Method method, Class<T> annotation) {
    while (true) {
      T ann = method.getAnnotation(annotation);
      if (ann != null) {
        return ann;
      }

      Class<?> aClass = method.getDeclaringClass();
      if (aClass == Object.class) {
        return null;
      }

      Class<?> superclass = aClass.getSuperclass();
      if (superclass == null) {
        return null;
      }

      try {
        method = superclass.getMethod(method.getName(), method.getParameterTypes());
      } catch (NoSuchMethodException e) {
        return null;
      }
    }
  }

  public static <T extends Annotation> T getAnnotation(Class<?> source, Class<T> annotation) {
    return getAnnotationInner(source, annotation, new HashSet<>());
  }

  private static <T extends Annotation> T getAnnotationInner(Class<?> source, Class<T> annotation, Set<Class<?>> cache) {

    if (source == null) {
      return null;
    }

    {
      T ann = source.getAnnotation(annotation);
      if (ann != null) {
        return ann;
      }
    }

    {
      if (cache.contains(source)) {
        return null;
      }
      cache.add(source);
    }

    for (Class<?> aClass : source.getInterfaces()) {
      T ann = getAnnotationInner(aClass, annotation, cache);
      if (ann != null) {
        return ann;
      }
    }

    return getAnnotationInner(source.getSuperclass(), annotation, cache);
  }

}
