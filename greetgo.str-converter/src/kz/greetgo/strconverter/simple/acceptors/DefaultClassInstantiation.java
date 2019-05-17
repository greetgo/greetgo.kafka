package kz.greetgo.strconverter.simple.acceptors;

import kz.greetgo.strconverter.simple.core.NameValue;

import java.util.Map;

public class DefaultClassInstantiation implements ClassInstantiation {

  @Override
  public Object createInstance(Class<?> workingClass,
                               Map<String, AttrAcceptor> acceptorMap,
                               NameValueList nameValueList
  ) {

    final Object object;

    try {
      object = workingClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
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
