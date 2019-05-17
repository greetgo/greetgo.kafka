package kz.greetgo.strconverter.simple.errors;

public class CannotSerializeClass extends RuntimeException {
  public final Class<?> serializingClass;

  public CannotSerializeClass(Class<?> serializingClass) {
    super("serializingClass = " + serializingClass);
    this.serializingClass = serializingClass;
  }

}
