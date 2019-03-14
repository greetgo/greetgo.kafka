package kz.greetgo.kafka.str;


import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.CompactWriter;
import kz.greetgo.strconverter.StrConverter;

import java.io.StringWriter;

public class StrConverterXml implements StrConverter {

  private final XStream xStream = new XStream();

  @Override
  public String toStr(Object object) {
    StringWriter stringWriter = new StringWriter();
    xStream.marshal(object, new CompactWriter(stringWriter));
    return stringWriter.toString();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T fromStr(String str) {
    return (T) xStream.fromXML(str);
  }

  @Override
  public void useClass(Class<?> aClass, String asName) {
    xStream.alias(asName, aClass);
  }
}
