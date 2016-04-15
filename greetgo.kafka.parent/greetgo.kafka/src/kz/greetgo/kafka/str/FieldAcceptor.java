package kz.greetgo.kafka.str;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class FieldAcceptor {

  private interface FieldGetter {
    Object get(Object source);
  }

  private interface FieldSetter {
    void set(Object target, Object value);
  }

  private final Map<String, FieldGetter> getterMap = new HashMap<>();

  private final Map<String, FieldSetter> setterMap = new HashMap<>();

  private static String extractFieldName(String fullName, int skipCount) {
    if (fullName.length() <= skipCount + 1) throw new RuntimeException();
    String part = fullName.substring(skipCount);
    if (part.length() == 1) return part.toLowerCase();
    return part.substring(0, 1).toLowerCase() + part.substring(1);
  }

  public FieldAcceptor(Class<?> oClass) {
    for (final Field field : oClass.getFields()) {
      getterMap.put(field.getName(), new FieldGetter() {
        @Override
        public Object get(Object source) {
          try {
            return field.get(source);
          } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
          }
        }
      });
      setterMap.put(field.getName(), new FieldSetter() {
        @Override
        public void set(Object target, Object value) {
          try {
            field.set(target, value);
          } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
          }
        }
      });
    }

    FOR:
    for (final Method method : oClass.getMethods()) {
      final int paramCount = method.getParameterTypes().length;
      final String methodName = method.getName();

      if (methodName.length() > 3 && methodName.startsWith("get") && paramCount == 0) {
        final String fieldName = extractFieldName(methodName, 3);

        getterMap.put(fieldName, new FieldGetter() {
          @Override
          public Object get(Object source) {
            try {
              return method.invoke(source);
            } catch (IllegalAccessException | InvocationTargetException e) {
              throw new RuntimeException(e);
            }
          }
        });

        continue FOR;
      }

      if (methodName.length() > 2 && methodName.startsWith("is") && paramCount == 0) {
        final String fieldName = extractFieldName(methodName, 2);

        getterMap.put(fieldName, new FieldGetter() {
          @Override
          public Object get(Object source) {
            try {
              return method.invoke(source);
            } catch (IllegalAccessException | InvocationTargetException e) {
              throw new RuntimeException(e);
            }
          }
        });

        continue FOR;
      }

      if (methodName.length() > 3 && methodName.startsWith("set") && paramCount == 1) {

        final String fieldName = extractFieldName(methodName, 3);

        setterMap.put(fieldName, new FieldSetter() {
          @Override
          public void set(Object target, Object value) {
            try {
              method.invoke(target, value);
            } catch (IllegalAccessException | InvocationTargetException e) {
              throw new RuntimeException(e);
            }
          }
        });

        continue FOR;

      }


    }

  }

  public void set(Object target, String fieldName, Object fieldValue) {
    FieldSetter fieldSetter = setterMap.get(fieldName);
    if (fieldSetter == null) return;
    fieldSetter.set(target, fieldValue);
  }

  public <T> T get(Object source, String fieldName) {
    FieldGetter fieldGetter = getterMap.get(fieldName);
    if (fieldGetter == null) return null;
    //noinspection unchecked
    return (T) fieldGetter.get(source);
  }
}
