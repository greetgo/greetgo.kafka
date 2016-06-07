package kz.greetgo.kafka.str.simple;

import kz.greetgo.kafka.str.StrConverter;

public class StrConverterSimple implements StrConverter {

  private final ConvertHelper convertHelper = new ConvertHelper();

  @Override
  public void useClass(Class<?> aClass, String alias) {
    convertHelper.useClass(aClass, alias);
  }

  @Override
  public String toStr(Object object) {
    return new Writer(convertHelper).write(object).result();
  }

  @Override
  public <T> T fromStr(String str) {
    return new Reader(convertHelper, str).read();
  }
}
