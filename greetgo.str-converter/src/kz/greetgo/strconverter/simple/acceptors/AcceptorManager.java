package kz.greetgo.strconverter.simple.acceptors;

import java.beans.Transient;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AcceptorManager {

  private Map<String, AttrAcceptor> acceptorMap = new HashMap<>();
  private List<String> orderList = new ArrayList<>();

  public AcceptorManager(Class<?> aClass) {
    getterMap = new HashMap<>();
    setterMap = new HashMap<>();

    fillWithFieldSetters(aClass);
    fillWithMethods(aClass);

    List<String> orderList2 = new ArrayList<>(orderList);
    orderList.clear();

    for (String name : orderList2) {
      if (acceptorMap.containsKey(name)) continue;

      AttrGetter getter = getterMap.get(name);
      AttrSetter setter = setterMap.get(name);

      if (getter != null && setter != null) {
        AttrAcceptor acceptor = new AttrAcceptor(getter, setter);
        acceptorMap.put(name, acceptor);
        orderList.add(name);
      }
    }

    getterMap = null;
    setterMap = null;
    orderList = Collections.unmodifiableList(orderList);
  }

  public List<String> orderList() {
    return orderList;
  }

  public AttrAcceptor acceptor(String name) {
    return acceptorMap.get(name);
  }

  private Map<String, AttrSetter> setterMap;
  private Map<String, AttrGetter> getterMap;

  private void fillWithFieldSetters(Class<?> aClass) {
    for (final Field field : aClass.getFields()) {
      if (field.getAnnotation(Transient.class) != null) continue;

      orderList.add(field.getName());

      setterMap.put(field.getName(), (target, value) -> {
        try {
          field.set(target, value);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      });

      getterMap.put(field.getName(), source -> {
        try {
          return field.get(source);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      });

    }
  }

  private void fillWithMethods(Class<?> aClass) {
    for (final Method method : aClass.getMethods()) {
      if (method.getAnnotation(Transient.class) != null) {
        continue;
      }

      int paramsCount = method.getParameterTypes().length;
      String methodName = method.getName();

      if (paramsCount == 0 && methodName.length() > 3 && methodName.startsWith("get")) {
        String normName = normName(methodName, 3);
        orderList.add(normName);
        getterMap.put(normName, source -> {
          try {
            return method.invoke(source);
          } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
          }
        });
      }

      Class<?> returnType = method.getReturnType();

      if (isBool(returnType) && paramsCount == 0 && methodName.length() > 2 && methodName.startsWith("is")) {
        String normName = normName(methodName, 2);
        orderList.add(normName);
        getterMap.put(normName, source -> {
          try {
            return method.invoke(source);
          } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
          }
        });
      }

      if (paramsCount == 1 && methodName.length() > 3 && methodName.startsWith("set")) {
        String normName = normName(methodName, 3);
        setterMap.put(normName, (target, value) -> {
          try {
            method.invoke(target, value);
          } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
          }
        });
      }

    }
  }

  private static boolean isBool(Class<?> type) {
    if (type == Boolean.TYPE) {
      return true;
    }
    if (type == Boolean.class) {
      return true;
    }
    return false;
  }

  private static String normName(String name, int prefixLen) {
    String name2 = name.substring(prefixLen);
    return name2.substring(0, 1).toLowerCase() + name2.substring(1);
  }

}
