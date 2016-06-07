package kz.greetgo.kafka.str.simple;

import kz.greetgo.util.RND;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.*;

import static org.fest.assertions.api.Assertions.assertThat;

public class StrConverterSimpleTest {

  StrConverterSimple converter;

  @BeforeMethod
  public void setUp() throws Exception {
    converter = new StrConverterSimple();
    converter.useClass(ExampleModel.class, "ExampleModel");
  }

  @Test
  public void toStr_fromStr() throws Exception {

    ExampleModel source = new ExampleModel();
    setSomeData(source);

    String str = converter.toStr(source);

    ExampleModel actual = converter.fromStr(str);

    assertThat(actual.toString()).isEqualTo(source.toString());
  }

  private void setSomeData(ExampleModel source) {
    source.strField = "qw1 qq";
    source.intField = 3213;
    source.longField = 432154;
    source.floatField = 543214;
    source.doubleField = 54325;
    source.dateField = new Date();
    source.charField = 's';
    source.byteField = (byte) 321;
    source.shortField = 321;
    source.exampleModelField = new ExampleModel();
    source.someObject = "11 22 33 4k 564";

    source.strFieldA = new String[]{"asd", "dsa"};
    source.intFieldA = new int[]{123, 543};
    source.longFieldA = new long[]{4324, 5435, 6546};
    source.floatFieldA = new float[]{5435, 654, 324, 23424};
    source.doubleFieldA = new double[]{43214, 3214, 5345, 34535, 32};
    source.dateFieldA = new Date[]{new Date(), new Date(), new Date()};
    source.charFieldA = new char[]{'s', 'd', 'f', 'r'};
    source.byteFieldA = new byte[]{(byte) 123, (byte) 432};
    source.shortFieldA = new short[]{123, 345, 21, 3542, 2};
    source.exampleModelFieldA = new ExampleModel[]{null, null, new ExampleModel()};
    source.objectA = new Object[]{"dsada", 134L, 2134.4, new BigDecimal("3213213")};

    source.listField = Arrays.asList("3213", "3213");
    source.setField = new HashSet<Object>(Arrays.asList("3213", "3213"));
    source.mapField = new HashMap<>();
    source.mapField.put("asd", "dsa");
    source.mapField.put(new ExampleModel(), 123L);
  }

  @Test
  public void intOne() throws Exception {

    int value = (RND.bool() ? 1 : -1) * RND.plusInt(100_000_000);

    String s = converter.toStr(value);

    int actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void longOne() throws Exception {

    long value = (RND.bool() ? 1 : -1) * RND.plusLong(100_000_000_000L);

    String s = converter.toStr(value);

    long actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void charOne() throws Exception {

    char value = (char) (32 + RND.plusInt(128 - 32));

    String s = converter.toStr(value);

    char actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void shortOne() throws Exception {

    short value = (short) ((RND.bool() ? 1 : -1) * RND.plusInt(100_000));

    String s = converter.toStr(value);

    short actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void byteOne() throws Exception {

    byte value = (byte) ((RND.bool() ? 1 : -1) * RND.plusInt(126));

    String s = converter.toStr(value);

    byte actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void floatOne() throws Exception {

    float value = (float) ((RND.bool() ? 1 : -1) * RND.plusDouble(1e10, 4));

    String s = converter.toStr(value);

    float actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void doubleOne() throws Exception {

    double value = ((RND.bool() ? 1 : -1) * RND.plusDouble(1e10, 4));

    String s = converter.toStr(value);

    double actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void dateOne() throws Exception {

    Date value = RND.dateYears(-10, +10);

    String s = converter.toStr(value);

    Date actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void strOne() throws Exception {

    String value = RND.str(10);

    String s = converter.toStr(value);

    String actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void strOne2() throws Exception {

    String value = RND.str(10) + "|" + RND.str(10);

    String s = converter.toStr(value);

    String actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void boolOne() throws Exception {

    boolean value = RND.bool();

    String s = converter.toStr(value);

    boolean actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void bdOne_1() throws Exception {

    BigDecimal value = new BigDecimal("123213213.2315435253453");

    String s = converter.toStr(value);

    BigDecimal actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void bdOne_2() throws Exception {

    BigDecimal value = new BigDecimal("-123213213e10234");

    String s = converter.toStr(value);

    BigDecimal actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void bdOne_3() throws Exception {

    BigDecimal value = new BigDecimal("-1232.13213e-10234");

    String s = converter.toStr(value);

    BigDecimal actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void nullOne() throws Exception {

    String s = converter.toStr(null);

    Object actual = converter.fromStr(s);

    assertThat(actual).isNull();

  }

  @Test
  @SuppressWarnings("unchecked")
  public void listOne() throws Exception {

    List value = new ArrayList();
    value.add("Asd");
    value.add(null);
    value.add(123L);

    String s = converter.toStr(value);

    List actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void mapOne() throws Exception {

    Map value = new HashMap();
    value.put("Asd", "dsa");
    value.put(111L, null);
    value.put("WOW", 123L);

    String s = converter.toStr(value);

    Map actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void setOne() throws Exception {

    Set value = new HashSet();
    value.add("Asd");
    value.add(null);
    value.add(123L);

    String s = converter.toStr(value);

    Set actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void intArray() throws Exception {
    int[] value = new int[]{RND.plusInt(10_000_000), RND.plusInt(10_000_000), RND.plusInt(10_000_000),};

    String s = converter.toStr(value);

    int[] actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void longArray() throws Exception {
    long[] value = new long[]{RND.plusInt(10_000_000), RND.plusInt(10_000_000), RND.plusInt(10_000_000),};

    String s = converter.toStr(value);

    long[] actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void doubleArray() throws Exception {
    double[] value = new double[]{RND.plusInt(10_000_000), RND.plusInt(10_000_000), RND.plusInt(10_000_000),};

    String s = converter.toStr(value);

    double[] actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void byteArray() throws Exception {
    byte[] value = new byte[]{(byte) RND.plusInt(100), (byte) RND.plusInt(100), (byte) RND.plusInt(100), (byte) RND.plusInt(100),};

    String s = converter.toStr(value);

    byte[] actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void charArray() throws Exception {
    char[] value = new char[]{(char) (32 + RND.plusInt(100)), (char) (32 + RND.plusInt(100)), (char) (32 + RND.plusInt(100)),};

    String s = converter.toStr(value);

    char[] actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void floatArray() throws Exception {
    float[] value = new float[]{RND.plusInt(10_000_000), RND.plusInt(10_000_000), RND.plusInt(10_000_000),};

    String s = converter.toStr(value);

    float[] actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void shortArray() throws Exception {
    short[] value = new short[]{(short) RND.plusInt(10_000_000), (short) RND.plusInt(10_000_000), (short) RND.plusInt(10_000_000),};

    String s = converter.toStr(value);

    short[] actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void strArray() throws Exception {
    String[] value = new String[]{RND.str(10), RND.str(10), RND.str(10), RND.str(10), RND.str(10), RND.str(10), RND.str(10),};

    String s = converter.toStr(value);

    String[] actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void objectArray() throws Exception {
    Object[] value = new Object[]{RND.str(10), RND.plusInt(10), null, RND.plusDouble(10_000, 4)};

    String s = converter.toStr(value);

    Object[] actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void listArray() throws Exception {
    List[] value = new List[]{new ArrayList(), null, null, new ArrayList()};
    value[0].add(RND.str(10));

    String s = converter.toStr(value);

    List[] actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void mapArray() throws Exception {
    Map[] value = new Map[]{new HashMap(), null, null, new HashMap()};
    value[0].put(RND.str(10), RND.str(10));

    String s = converter.toStr(value);

    Map[] actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void setArray() throws Exception {
    Set[] value = new Set[]{new HashSet(), null, null, new HashSet()};
    value[0].add(RND.str(10));

    String s = converter.toStr(value);

    Set[] actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }
}