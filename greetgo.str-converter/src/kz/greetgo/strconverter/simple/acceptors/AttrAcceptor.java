package kz.greetgo.strconverter.simple.acceptors;

public class AttrAcceptor {
  private final AttrGetter getter;
  private final AttrSetter setter;

  public AttrAcceptor(AttrGetter getter, AttrSetter setter) {
    this.getter = getter;
    this.setter = setter;
  }

  public void set(Object target, Object value) {
    setter.set(target, value);
  }

  public Object get(Object source) {
    return getter.get(source);
  }
}
