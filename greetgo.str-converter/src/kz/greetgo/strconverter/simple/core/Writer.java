package kz.greetgo.strconverter.simple.core;

import kz.greetgo.strconverter.simple.acceptors.AcceptorManager;
import kz.greetgo.strconverter.simple.errors.CannotSerializeClass;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Serialize object and write it to string.
 *
 * Single threaded - you cannot use this class from different threads
 */
public class Writer {
  private ConvertHelper convertHelper;

  private StringBuilder res = new StringBuilder(4 * 1024);

  public Writer(ConvertHelper convertHelper) {
    this.convertHelper = convertHelper;
  }

  public Writer write(Object object) {
    write0(object);
    return this;
  }

  public String result() {
    return res.toString();
  }

  private void write0(Object object) {
    if (object == null) {
      res.append('N');
      return;
    }

    Class<?> objectClass = object.getClass();

    if (objectClass == Integer.class || objectClass == Integer.TYPE) {
      res.append('I').append(object);
      return;
    }

    if (objectClass == Long.class || objectClass == Long.TYPE) {
      res.append('L').append(object);
      return;
    }

    if (objectClass == Character.class || objectClass == Character.TYPE) {
      res.append('C').append(object);
      return;
    }

    if (objectClass == Byte.class || objectClass == Byte.TYPE) {
      res.append('B').append(object);
      return;
    }

    if (objectClass == Float.class || objectClass == Float.TYPE) {
      res.append('F').append(object);
      return;
    }

    if (objectClass == Double.class || objectClass == Double.TYPE) {
      res.append('U').append(object);
      return;
    }

    if (objectClass == Boolean.class || objectClass == Boolean.TYPE) {
      res.append((boolean) object ? "J" : "K");
      return;
    }

    if (objectClass == Short.class || objectClass == Short.TYPE) {
      res.append('O').append(object);
      return;
    }

    if (objectClass == String.class) {
      res.append('S').append(quote((String) object));
      return;
    }

    if (objectClass == BigDecimal.class) {
      res.append('X').append(object);
      return;
    }

    if (Date.class.isAssignableFrom(objectClass)) {
      res.append('D').append(sdf.format((Date) object));
      return;
    }

    if (List.class.isAssignableFrom(objectClass)) {
      List list = (List) object;
      res.append("P[");
      for (Object o : list) {
        write0(o);
      }
      res.append(']');
      return;
    }

    if (Set.class.isAssignableFrom(objectClass)) {
      Set set = (Set) object;
      res.append("G[");
      for (Object o : set) {
        write0(o);
      }
      res.append(']');
      return;
    }

    if (Map.class.isAssignableFrom(objectClass)) {
      @SuppressWarnings("unchecked")
      Map<Object, Object> map = (Map<Object, Object>) object;
      res.append("M[");
      for (Map.Entry<Object, Object> e : map.entrySet()) {
        write0(e.getKey());
        write0(e.getValue());
      }
      res.append(']');
      return;
    }

    if (objectClass.isArray()) {
      String arrayType = getArrayTypeId(objectClass);
      int length = Array.getLength(object);
      res.append('A').append(length).append(arrayType).append('[');
      for (int i = 0; i < length; i++) {
        write0(Array.get(object, i));
      }
      res.append(']');
      return;
    }

    {
      String alias = convertHelper.classAliasMap.get(objectClass);
      if (alias != null) {

        if (objectClass.isEnum()) {
          Enum<?> e = (Enum<?>) object;
          res.append('Q').append(alias).append('{').append(e.name()).append('}');
          return;
        }


        res.append('Q').append(alias).append('{');
        AcceptorManager acceptorManager = convertHelper.getAcceptorManager(objectClass);

        int len = res.length();

        for (String attrName : acceptorManager.orderList()) {
          Object attrValue = acceptorManager.acceptor(attrName).get(object);
          if (attrValue != null) {
            res.append(attrName).append('=');
            write0(attrValue);
            res.append(',');
          }
        }

        if (len < res.length()) res.setLength(res.length() - 1);

        res.append('}');

        return;
      }
    }

    throw new CannotSerializeClass(object.getClass());
  }

  private String getArrayTypeId(Class<?> objectClass) {

    if (objectClass == int[].class) return "I";
    if (objectClass == long[].class) return "L";
    if (objectClass == short[].class) return "O";
    if (objectClass == char[].class) return "C";
    if (objectClass == byte[].class) return "B";
    if (objectClass == boolean[].class) return "J";
    if (objectClass == BigDecimal[].class) return "X";
    if (objectClass == double[].class) return "U";
    if (objectClass == float[].class) return "F";
    if (objectClass == Object[].class) return "Q";
    if (objectClass == String[].class) return "S";
    if (objectClass == List[].class) return "P";
    if (objectClass == Date[].class) return "D";
    if (objectClass == Map[].class) return "M";
    if (objectClass == Set[].class) return "G";

    return "H" + convertHelper.getAliasForOrThrowError(objectClass.getComponentType());
  }

  private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

  private String quote(String str) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0, n = str.length(); i < n; i++) {
      char c = str.charAt(i);
      switch (c) {

        case '\\':
          sb.append("\\\\");
          break;

        case '|':
          sb.append("\\|");
          break;

        default:
          sb.append(c);
          break;
      }
    }
    sb.append('|');
    return sb.toString();
  }
}
