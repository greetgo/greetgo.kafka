package kz.greetgo.strconverter.simple.errors;

public class ClassAlreadyRegistered extends RuntimeException {

  public final Class aClass;
  public final String registeringAlias;
  public final String alreadyRegisteredAlias;

  public ClassAlreadyRegistered(Class aClass, String registeringAlias, String alreadyRegisteredAlias) {
    super("Alias `" + registeringAlias + "` is registering to a class `"
      + aClass + "`, but it class already registered as alias `" + alreadyRegisteredAlias + "`");
    this.aClass = aClass;
    this.registeringAlias = registeringAlias;
    this.alreadyRegisteredAlias = alreadyRegisteredAlias;
  }
}
