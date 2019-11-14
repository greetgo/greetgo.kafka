package kz.greetgo.strconverter.simple.errors;

public class CannotInstantiateClass extends RuntimeException {
  public final Class<?> instantiatingClass;

  public CannotInstantiateClass(Class<?> instantiatingClass, Throwable cause) {
    super("instantiatingClass = " + instantiatingClass, cause);
    this.instantiatingClass = instantiatingClass;
  }
}
