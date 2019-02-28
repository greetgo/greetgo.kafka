package kz.greetgo.strconverter.simple;

import java.util.*;

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

  @SuppressWarnings("unchecked")
  private String str(Map<Object, Object> mapField) {
    if (mapField == null) return "null";
    List keys = new ArrayList<>();
    keys.addAll((Set) mapField.keySet());
    Collections.sort(keys, new Comparator() {
      @Override
      public int compare(Object o1, Object o2) {
        return o1.hashCode() - o2.hashCode();
      }
    });
    StringBuilder sb = new StringBuilder();
    sb.append('{');
    for (Object key : keys) {
      sb.append(key).append('=').append(mapField.get(key)).append(',');
    }
    sb.append('}');
    return sb.toString();
  }
}
