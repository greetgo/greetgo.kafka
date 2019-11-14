package kz.greetgo.strconverter.simple.acceptors;

import kz.greetgo.strconverter.simple.core.NameValue;
import kz.greetgo.strconverter.simple.errors.CannotInstantiateClass;

import java.beans.ConstructorProperties;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DefaultClassInstantiation implements ClassInstantiation {

  @Override
  public Object createInstance(Class<?> workingClass,
                               Map<String, AttrAcceptor> acceptorMap,
                               NameValueList nameValueList
  ) {

    {

      List<Constructor<?>> constructors = new ArrayList<>();

      for (Constructor<?> constructor : workingClass.getConstructors()) {
        ConstructorProperties properties = constructor.getAnnotation(ConstructorProperties.class);
        if (properties == null) {
          continue;
        }

        if (constructors.isEmpty()) {
          constructors.add(constructor);
        } else {

          int maxLength = constructors.get(0).getAnnotation(ConstructorProperties.class).value().length;
          int length = properties.value().length;

          if (maxLength == length) {
            constructors.add(constructor);
          } else if (maxLength < length) {
            constructors.clear();
            constructors.add(constructor);
          }

        }
      }

      if (constructors.size() > 0) {
        Constructor<?> constructor = constructors.get(constructors.size() - 1);
        ConstructorProperties properties = constructor.getAnnotation(ConstructorProperties.class);

        Set<String> usedNames = new HashSet<>();
        Object[] constructorArgValues = new Object[properties.value().length];
        for (int i = 0, c = constructorArgValues.length; i < c; i++) {
          String name = properties.value()[i];
          usedNames.add(name);
          Object value = nameValueList.getValue(name);
          constructorArgValues[i] = value;
        }

        final Object object;

        try {
          object = constructor.newInstance(constructorArgValues);
        } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
          throw new CannotInstantiateClass(workingClass, e);
        }

        for (NameValue nameValue : nameValueList.list()) {
          if (usedNames.contains(nameValue.name)) {
            continue;
          }
          AttrAcceptor attrAcceptor = acceptorMap.get(nameValue.name);
          if (attrAcceptor != null) {
            attrAcceptor.set(object, nameValue.value);
          }
        }

        return object;

      }

    }


    {
      final Object object;

      try {
        object = workingClass.getConstructor().newInstance();
      } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
        throw new CannotInstantiateClass(workingClass, e);
      }

      for (NameValue nameValue : nameValueList.list()) {
        AttrAcceptor attrAcceptor = acceptorMap.get(nameValue.name);
        if (attrAcceptor != null) {
          attrAcceptor.set(object, nameValue.value);
        }
      }

      return object;
    }

  }

}
