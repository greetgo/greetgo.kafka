package kz.greetgo.strconverter.simple.acceptors;

import java.util.Map;

public interface ClassInstantiation {

  Object createInstance(Class<?> workingClass, Map<String, AttrAcceptor> acceptorMap, NameValueList nameValueList);

}
