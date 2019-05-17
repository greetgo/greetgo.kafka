package kz.greetgo.strconverter.simple.acceptors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class CustomClassManager implements ClassManager {

  private final Class<?> aClass;
  private final String alias;

  @Override
  public Class<?> workingClass() {
    return aClass;
  }

  @Override
  public String alias() {
    return alias;
  }

  private CustomClassManager(Class<?> aClass, String alias) {
    this.aClass = aClass;
    this.alias = alias;
  }

  public static CustomClassManager of(Class<?> aClass) {
    return of(aClass, aClass.getSimpleName());
  }

  public static CustomClassManager of(Class<?> aClass, String alias) {
    return new CustomClassManager(aClass, alias);
  }

  public CustomClassManager addAcceptor(String attrName, AttrAcceptor attrAcceptor) {
    requireNonNull(attrAcceptor);
    requireNonNull(attrName);
    if (acceptorMap.containsKey(attrName)) {
      throw new IllegalArgumentException("Attribute `" + attrName + "` already registered");
    }

    orderList.add(attrName);
    acceptorMap.put(attrName, attrAcceptor);
    return this;
  }

  public CustomClassManager addGetterAndSetter(String attrName, AttrGetter attrGetter, AttrSetter attrSetter) {
    return addAcceptor(attrName, new AttrAcceptor(requireNonNull(attrGetter), attrSetter));
  }

  public CustomClassManager addOnlyGetter(String attrName, AttrGetter attrGetter) {
    return addGetterAndSetter(attrName, requireNonNull(attrGetter), null);
  }

  private final List<String> orderList = new ArrayList<>();
  private final Map<String, AttrAcceptor> acceptorMap = new HashMap<>();
  private ClassInstantiation classInstantiation = new DefaultClassInstantiation();

  public CustomClassManager setClassInstantiation(ClassInstantiation classInstantiation) {
    this.classInstantiation = classInstantiation;
    return this;
  }

  @Override
  public Object createInstance(NameValueList nameValueList) {
    return classInstantiation.createInstance(workingClass(), acceptorMap, nameValueList);
  }

  @Override
  public List<String> orderList() {
    return orderList;
  }

  @Override
  public AttrAcceptor acceptor(String name) {
    return acceptorMap.get(name);
  }
}
