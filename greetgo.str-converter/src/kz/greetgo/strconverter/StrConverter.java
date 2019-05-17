package kz.greetgo.strconverter;

public interface StrConverter {

  String toStr(Object object);

  <T> T fromStr(String str);

}
