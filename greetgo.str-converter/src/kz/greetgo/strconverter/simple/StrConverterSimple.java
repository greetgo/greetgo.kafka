package kz.greetgo.strconverter.simple;

import kz.greetgo.strconverter.StrConverter;
import kz.greetgo.strconverter.simple.core.ConvertHelper;
import kz.greetgo.strconverter.simple.core.Reader;
import kz.greetgo.strconverter.simple.core.Writer;

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
