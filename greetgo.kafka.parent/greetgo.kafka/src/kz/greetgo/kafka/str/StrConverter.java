package kz.greetgo.kafka.str;

public interface StrConverter {

  String toStr(Object object);

  <T> T fromStr(String str);

  void useClass(Class<?> aClass, String asName);

}
