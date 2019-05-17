package kz.greetgo.strconverter.simple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Comparator.comparingInt;

public class ExampleModel {

  public String strField;
  public int intField;
  public long longField;
  public float floatField;
  public double doubleField;
  public Date dateField;
  public char charField;
  public byte byteField;
  public short shortField;
  public ExampleModel exampleModelField;
  public Object someObject;

  public String[] strFieldA;
  public int[] intFieldA;
  public long[] longFieldA;
  public float[] floatFieldA;
  public double[] doubleFieldA;
  public Date[] dateFieldA;
  public char[] charFieldA;
  public byte[] byteFieldA;
  public short[] shortFieldA;
  public ExampleModel[] exampleModelFieldA;
  public Object[] objectA;

  public List<String> listField;
  public Set<Object> setField;
  public Map<Object, Object> mapField;

  @Override
  public String toString() {
    return "ExampleModel{" +
      "strField='" + strField + '\'' +
      ", intField=" + intField +
      ", longField=" + longField +
      ", floatField=" + floatField +
      ", doubleField=" + doubleField +
      ", dateField=" + dateField +
      ", charField=" + charField +
      ", byteField=" + byteField +
      ", shortField=" + shortField +
      ", exampleModelField=" + exampleModelField +
      ", someObject=" + someObject +
      ", strFieldA=" + Arrays.toString(strFieldA) +
      ", intFieldA=" + Arrays.toString(intFieldA) +
      ", longFieldA=" + Arrays.toString(longFieldA) +
      ", floatFieldA=" + Arrays.toString(floatFieldA) +
      ", doubleFieldA=" + Arrays.toString(doubleFieldA) +
      ", dateFieldA=" + Arrays.toString(dateFieldA) +
      ", charFieldA=" + Arrays.toString(charFieldA) +
      ", byteFieldA=" + Arrays.toString(byteFieldA) +
      ", shortFieldA=" + Arrays.toString(shortFieldA) +
      ", exampleModelFieldA=" + Arrays.toString(exampleModelFieldA) +
      ", objectA=" + Arrays.toString(objectA) +
      ", listField=" + listField +
      ", setField=" + setField +
      ", mapField=" + str(mapField) +
      '}';
  }


  private String str(Map<Object, Object> mapField) {

    if (mapField == null) {
      return "null";
    }

    List<Object> keys = new ArrayList<>(mapField.keySet());

    keys.sort(comparingInt(Object::hashCode));

    StringBuilder sb = new StringBuilder(128);

    sb.append('{');

    for (Object key : keys) {
      sb.append(key).append('=').append(mapField.get(key)).append(',');
    }

    sb.append('}');

    return sb.toString();

  }

}
