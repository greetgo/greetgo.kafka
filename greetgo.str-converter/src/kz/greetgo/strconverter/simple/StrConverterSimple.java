package kz.greetgo.strconverter.simple;

import kz.greetgo.strconverter.StrConverter;
import kz.greetgo.strconverter.simple.core.ConvertRegistry;
import kz.greetgo.strconverter.simple.core.Reader;
import kz.greetgo.strconverter.simple.core.Writer;

public class StrConverterSimple implements StrConverter {

  private final ConvertRegistry convertRegistry = new ConvertRegistry();

  public ConvertRegistry convertRegistry() {
    return convertRegistry;
  }

  @Override
  public String toStr(Object object) {
    return new Writer(convertRegistry).write(object).result();
  }

  @Override
  public <T> T fromStr(String str) {
    return new Reader(convertRegistry, str).read();
  }

}
