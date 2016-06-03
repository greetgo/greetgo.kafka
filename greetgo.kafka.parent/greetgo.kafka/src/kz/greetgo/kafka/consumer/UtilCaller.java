package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.core.Box;
import kz.greetgo.kafka.core.Head;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class UtilCaller {

  public static Caller createCaller(final Object bean, final Method method) {
    Type[] parameterTypes = method.getGenericParameterTypes();

    if (parameterTypes.length == 1 && isListOfBoxes(parameterTypes[0])) {
      return new Caller() {
        @Override
        public void call(List<Box> list) {
          try {
            method.invoke(bean, list);
          } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
          }
        }
      };
    }

    if (parameterTypes.length == 1 && isList(parameterTypes[0])) {
      return new Caller() {
        @Override
        public void call(List<Box> list) {
          List<Object> objectList = new ArrayList<>(list.size());
          for (Box box : list) {
            extractBodiesAndAdd(objectList, box);
          }
          try {
            method.invoke(bean, objectList);
          } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
          }
        }
      };
    }

    if (parameterTypes.length == 1 && parameterTypes[0] == Box.class) {
      return new Caller() {
        @Override
        public void call(List<Box> list) {
          for (Box box : list) {
            try {
              method.invoke(bean, box);
            } catch (IllegalAccessException | InvocationTargetException e) {
              throw new RuntimeException(e);
            }
          }
        }
      };
    }

    if (parameterTypes.length == 1) {
      return new Caller() {
        @Override
        public void call(List<Box> list) {
          for (Box box : list) {
            try {
              method.invoke(bean, box.body);
            } catch (IllegalAccessException | InvocationTargetException e) {
              throw new RuntimeException(e);
            }
          }
        }
      };
    }

    if (parameterTypes.length == 2 && parameterTypes[1] == Head.class) {
      return new Caller() {
        @Override
        public void call(List<Box> list) {
          for (Box box : list) {
            try {
              method.invoke(bean, box.body, box.head);
            } catch (IllegalAccessException | InvocationTargetException e) {
              throw new RuntimeException(e);
            }
          }
        }
      };
    }

    throw new RuntimeException("Cannot create caller for " + method.toGenericString() + "\n" +
        "You can use following variants:\n" +
        "@" + Consume.class.getSimpleName() + " void anyName(List<Box> list)...\n" +
        "@" + Consume.class.getSimpleName() + " void anyName(List<SomeClass> list)...\n" +
        "@" + Consume.class.getSimpleName() + " void anyName(Box box) ...\n" +
        "@" + Consume.class.getSimpleName() + " void anyName(SomeClass asd) ...\n" +
        "@" + Consume.class.getSimpleName() + " void anyName(SomeClass asd, Head head) ...\n" +
        "* Box - it is " + Box.class.getName() + "\n" +
        "* Head - it is " + Head.class.getName() + "\n" +
        "* SomeClass - it is some class except Box or Head");
  }

  private static boolean isListOfBoxes(Type type) {
    if (!(type instanceof ParameterizedType)) return false;
    ParameterizedType parameterizedType = (ParameterizedType) type;
    if (parameterizedType.getRawType() != List.class) return false;
    Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
    if (actualTypeArguments.length != 1) return false;
    return actualTypeArguments[0] == Box.class;
  }

  private static boolean isList(Type type) {
    if (type == List.class) return true;
    if (!(type instanceof ParameterizedType)) return false;
    ParameterizedType parameterizedType = (ParameterizedType) type;
    return parameterizedType.getRawType() == List.class;
  }

  private static void extractBodiesAndAdd(List<Object> objectList, Object object) {
    if (object instanceof Box) {
      object = ((Box) object).body;
    }

    if (object instanceof List) {
      for (Object subObject : (List) object) {
        extractBodiesAndAdd(objectList, subObject);
      }
      return;
    }

    objectList.add(object);
  }
}
