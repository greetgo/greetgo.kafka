package kz.greetgo.strconverter.simple.acceptors;

import kz.greetgo.strconverter.simple.core.NameValue;

import java.util.List;
import java.util.Map;

public interface ClassInstantiation {

  Object createInstance(Class<?> workingClass, Map<String, AttrAcceptor> acceptorMap, NameValueList nameValueList);

}
