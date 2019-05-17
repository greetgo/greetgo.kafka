package kz.greetgo.strconverter.simple.acceptors;

import kz.greetgo.strconverter.simple.core.NameValue;

import java.util.List;

public interface ClassManager {

  List<String> orderList();

  AttrAcceptor acceptor(String name);

  Class<?> workingClass();

  String alias();

  Object createInstance(NameValueList nameValueList);

}
