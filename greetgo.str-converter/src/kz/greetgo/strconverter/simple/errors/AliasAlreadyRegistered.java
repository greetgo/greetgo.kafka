package kz.greetgo.strconverter.simple.errors;

public class AliasAlreadyRegistered extends RuntimeException {

  public final String alias;
  public final Class registeringClass;
  public final Class alreadyRegisteredClass;

  public AliasAlreadyRegistered(String alias, Class registeringClass, Class alreadyRegisteredClass) {
    super("Alias `" + alias + "` is registering to class `" + registeringClass
      + "` but it already registered for class `" + alreadyRegisteredClass + "`");
    this.alias = alias;
    this.registeringClass = registeringClass;
    this.alreadyRegisteredClass = alreadyRegisteredClass;
  }
}
