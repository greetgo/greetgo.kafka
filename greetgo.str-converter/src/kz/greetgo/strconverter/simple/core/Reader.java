package kz.greetgo.strconverter.simple.core;

import kz.greetgo.strconverter.simple.acceptors.NameValueList;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static kz.greetgo.strconverter.simple.core.ConvertRegistry.isJavaVariableChar;

/**
 * Reading object from serialised string.
 * <p>
 * Single threaded - you cannot use this class from different threads
 */
public class Reader {
  private final ConvertRegistry convertRegistry;
  private final char[] source;

  private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

  private int index = 0;

  public Reader(ConvertRegistry convertRegistry, String sourceStr) {
    this.convertRegistry = convertRegistry;
    this.source = sourceStr.toCharArray();
  }

  @SuppressWarnings("unchecked")
  public <T> T read() {
    try {
      return (T) read0();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Object read0() throws Exception {
    char commandChar = source[index++];
    switch (commandChar) {
      case 'N':
        return null;
      case 'I':
        return Integer.valueOf(readNumStr());
      case 'L':
        return Long.valueOf(readNumStr());
      case 'C':
        return source[index++];
      case 'O':
        return Short.valueOf(readNumStr());
      case 'B':
        return Byte.valueOf(readNumStr());
      case 'F':
        return Float.valueOf(readNumStr());
      case 'U':
        return Double.valueOf(readNumStr());
      case 'D':
        return sdf.parse(readDateStr());
      case 'S':
        return readAndUnquoteStr();
      case 'J':
        return true;
      case 'K':
        return false;
      case 'X':
        return new BigDecimal(readNumStr());
      case 'A':
        return readArray(Integer.valueOf(readNumStr()));
      case 'P':
        return readListAsArrayList();
      case 'G':
        return readSetAsHashSet();
      case 'M':
        return readMapAsHashMap();
      case 'Q':
        return readObjectByAlias();
      case 'b':
        return readByteArray();
      case 'c':
        return readAndUnquoteStr().toCharArray();
    }
    throw new RuntimeException("Illegal command char " + commandChar);
  }

  private Object readByteArray() {
    String base64str = readAndUnquoteStr();
    return Base64.getDecoder().decode(base64str);
  }


  private Object readMapAsHashMap() throws Exception {
    char openBrace = source[index++];
    if (openBrace != '[') throw new RuntimeException("YTWhS7HDW7U: Here must be char [");
    HashMap<Object, Object> ret = new HashMap<>();
    while (source[index] != ']') {
      ret.put(read0(), read0());
    }
    index++;
    return ret;
  }

  private Object readSetAsHashSet() throws Exception {
    char openBrace = source[index++];
    if (openBrace != '[') throw new RuntimeException("QTR72J6TGF: Here must be char [");
    HashSet<Object> ret = new HashSet<>();
    while (source[index] != ']') {
      ret.add(read0());
    }
    index++;
    return ret;
  }

  private Object readListAsArrayList() throws Exception {
    char openBrace = source[index++];
    if (openBrace != '[') throw new RuntimeException("BATGR517J: Here must be char [");
    ArrayList<Object> ret = new ArrayList<>();
    while (source[index] != ']') {
      ret.add(read0());
    }
    index++;
    return ret;
  }

  private Object readArray(int arraySize) throws Exception {
    Object ret = createArray(arraySize);
    char openBrace = source[index++];
    if (openBrace != '[') throw new RuntimeException("AR5162YWQ: Here must be char [");
    for (int i = 0; i < arraySize; i++) {
      Array.set(ret, i, read0());
    }
    char closeBrace = source[index++];
    if (closeBrace != ']') throw new RuntimeException("Q76GR231: Here must be char ]");
    return ret;
  }

  private Object createArray(int arraySize) {
    char typeChar = source[index++];
    switch (typeChar) {
      case 'S':
        return new String[arraySize];
      case 'I':
        return new int[arraySize];
      case 'L':
        return new long[arraySize];
      case 'O':
        return new short[arraySize];
      case 'C':
        return new char[arraySize];
      case 'B':
        return new byte[arraySize];
      case 'D':
        return new Date[arraySize];
      case 'F':
        return new float[arraySize];
      case 'U':
        return new double[arraySize];
      case 'Q':
        return new Object[arraySize];
      case 'J':
        return new boolean[arraySize];
      case 'X':
        return new BigDecimal[arraySize];
      case 'P':
        return new List[arraySize];
      case 'G':
        return new Set[arraySize];
      case 'M':
        return new Map[arraySize];
      case 'H': {
        String alias = readJavaId();
        Class<?> aClass = convertRegistry.aliasClassMap.get(alias);
        if (aClass == null) return new Object[arraySize];
        return Array.newInstance(aClass, arraySize);
      }
    }
    throw new RuntimeException("Cannot create array for " + typeChar);
  }

  private String readAndUnquoteStr() {
    char[] source = this.source;
    int length = source.length;
    int i = index;

    StringBuilder sb = new StringBuilder();
    char prev = 0;

    while (i < length) {
      char c = source[i++];
      if (c == '\\') {
        if (prev == '\\') sb.append('\\');
      } else if (c == '|') {
        if (prev == '\\') {
          sb.append('|');
        } else {
          break;
        }
      } else {
        sb.append(c);
      }
      prev = c;
    }

    index = i;
    return sb.toString();
  }

  private String readNumStr() {
    char[] source = this.source;
    int length = source.length;
    int i1 = index;
    int i2 = index;
    while (i2 < length && isNumChar(source[i2])) {
      i2++;
    }
    index = i2;
    return new String(source, i1, i2 - i1);
  }

  private String readDateStr() {
    char[] source = this.source;
    int length = source.length;
    int i1 = index;
    int i2 = index;
    while (i2 < length && isDateChar(source[i2])) {
      i2++;
    }
    index = i2;
    return new String(source, i1, i2 - i1);
  }

  private boolean isDateChar(char c) {
    if ('0' <= c && c <= '9') return true;
    switch (c) {
      case 'T':
      case '-':
      case ':':
      case '.':
        return true;
    }
    return false;
  }

  private boolean isNumChar(char c) {
    if ('0' <= c && c <= '9') return true;
    switch (c) {
      case '-':
      case '+':
      case '.':
      case 'E':
      case 'e':
        return true;
    }
    return false;
  }

  private Object readObjectByAlias() throws Exception {
    String alias = readJavaId();
    char openBrace = source[index++];
    if (openBrace != '{') {
      throw new RuntimeException("AQ87A2K8GYT: Here must be char {");
    }

    Class<?> objectClass = convertRegistry.aliasClassMap.get(alias);
    if (objectClass == null) {
      throw new RuntimeException("No alias " + alias);
    }

    if (objectClass.isEnum()) {
      String enumStrValue = readJavaId();
      char closeBrace = source[index++];
      if (closeBrace != '}') {
        throw new RuntimeException("AJYRw8U: Here must be char }");
      }

      try {
        @SuppressWarnings("unchecked")
        Object enumValue = Enum.valueOf((Class<? extends Enum>) objectClass, enumStrValue);
        return enumValue;
      } catch (IllegalArgumentException | NullPointerException ignore) {
        return null;
      }
    }

    final NameValueList nameValueList = new NameValueList();

    while (index < source.length) {
      char c = source[index];
      if (c == '}') {
        index++;
        break;
      }

      String name = readJavaId();
      char eq = source[index++];
      if (eq != '=') {
        throw new RuntimeException("Here must be =");
      }
      Object value = read0();
      if (source[index] == ',') {
        index++;
      }

      nameValueList.add(name, value);
    }

    return convertRegistry
      .getAcceptorManager(alias)
      .createInstance(nameValueList);

  }

  private String readJavaId() {
    StringBuilder sb = new StringBuilder();

    while (index < source.length) {
      char c = source[index];
      if (!isJavaVariableChar(c)) break;
      index++;
      sb.append(c);
    }

    return sb.toString();
  }
}
