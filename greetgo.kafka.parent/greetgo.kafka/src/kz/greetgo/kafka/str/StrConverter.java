package kz.greetgo.kafka.str;

import java.io.Reader;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

public class StrConverter {

  private final Map<String, Class<?>> usingClasses = new HashMap<String, Class<?>>();

  public void marshall(Object object, Writer writer) {

  }

  public Object unMarshall(Reader reader) {
    return null;
  }

  public void useClass(String name, Class<?> aClass) {

  }
}
