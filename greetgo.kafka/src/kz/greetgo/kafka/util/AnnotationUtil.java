package kz.greetgo.kafka.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
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

  public static Annotation[][] getParameterAnnotations(Method method) {
    Objects.requireNonNull(method);

    List<Annotation[][]> all = new ArrayList<>();

    putParameterAnnotations(method, all);

    Annotation[][] ret = new Annotation[method.getParameterCount()][];

    int[] totalLengths = new int[ret.length];
    for (Annotation[][] annotations : all) {
      for (int i = 0; i < ret.length; i++) {
        totalLengths[i] += annotations[i].length;
      }
    }
    for (int i = 0; i < ret.length; i++) {
      ret[i] = new Annotation[totalLengths[i]];
    }

    int[] indexes = new int[ret.length];
    for (Annotation[][] annotations : all) {
      for (int i = 0; i < ret.length; i++) {
        for (int j = 0; j < annotations[i].length; j++) {
          ret[i][indexes[i]++] = annotations[i][j];
        }
      }
    }

    return ret;
  }

  private static void putParameterAnnotations(Method method, List<Annotation[][]> dest) {

    Set<Class<?>> cache = new HashSet<>();

    Class<?> current = method.getDeclaringClass();

    while (current != null) {

      if (cache.contains(current)) {
        return;
      }
      cache.add(current);

      try {
        Method m = current.getDeclaredMethod(method.getName(), method.getParameterTypes());
        dest.add(m.getParameterAnnotations());
      } catch (NoSuchMethodException ignore) {
      }

      current = current.getSuperclass();
    }
  }

}
