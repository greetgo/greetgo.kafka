package kz.greetgo.strconverter.simple.errors;

public class NoRegisteredClassForAlias extends RuntimeException {
  public final String alias;

  public NoRegisteredClassForAlias(String alias) {
    super("alias = " + alias);
    this.alias = alias;
  }
}
