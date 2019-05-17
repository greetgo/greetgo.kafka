package kz.greetgo.strconverter;

public interface StrConverter {

  String toStr(Object object);

  <T> T fromStr(String str);

  void useClass(Class<?> aClass, String asName);

  default void useClass(Class<?> aClass) {
    useClass(aClass, aClass.getSimpleName());
  }

}
