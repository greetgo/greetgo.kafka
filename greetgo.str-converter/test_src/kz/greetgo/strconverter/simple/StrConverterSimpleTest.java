package kz.greetgo.strconverter.simple;

import kz.greetgo.strconverter.simple.acceptors.CustomClassManager;
import kz.greetgo.util.RND;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.fest.assertions.api.Assertions.assertThat;

public class StrConverterSimpleTest {

  StrConverterSimple converter;

  @BeforeMethod
  public void prepareConverter() {
    converter = new StrConverterSimple();
    converter.convertRegistry().register(ExampleModel.class, "ExampleModel");
    converter.convertRegistry().register(ForTest.class, "ForTest");
    converter.convertRegistry().register(TestEnum.class, "TestEnum");
    converter.convertRegistry().register(TestEnumAbstract.class, "TestEnumAbstract");
    converter.convertRegistry().register(ManyFieldClass.class, "ManyFieldClass");
    converter.convertRegistry().register(OnlyFinalFieldsClass.class, "OnlyFinalFieldsClass");
    converter.convertRegistry().register(WithFinalFieldsClass.class, "WithFinalFieldsClass");
    converter.convertRegistry().register(ClassWithBoolean.class, "ClassWithBoolean");
  }

  @Test
  public void toStr_fromStr() {

    ExampleModel source = new ExampleModel();
    setSomeData(source);

    String str = converter.toStr(source);

    System.out.println("4gfc256dfx :: str = `" + str + "`");

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
    source.objectA = new Object[]{"dsa1da", 134L, 2134.4, new BigDecimal("3213213")};

    source.listField = Arrays.asList("3213", "3213");
    source.setField = new HashSet<>(Arrays.asList("3213", "3213"));
    source.mapField = new HashMap<>();
    source.mapField.put("asd", "dsa");
    source.mapField.put(new ExampleModel(), 123L);
  }

  @Test
  public void intOne() {

    int value = (RND.bool() ? 1 : -1) * RND.plusInt(100_000_000);

    String s = converter.toStr(value);

    int actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void longOne() {

    long value = (RND.bool() ? 1 : -1) * RND.plusLong(100_000_000_000L);

    String s = converter.toStr(value);

    long actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void charOne() {

    char value = (char) (32 + RND.plusInt(128 - 32));

    String s = converter.toStr(value);

    char actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void shortOne() {

    short value = (short) ((RND.bool() ? 1 : -1) * RND.plusInt(100_000));

    String s = converter.toStr(value);

    short actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void byteOne() {

    byte value = (byte) ((RND.bool() ? 1 : -1) * RND.plusInt(126));

    String s = converter.toStr(value);

    byte actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void floatOne() {

    float value = (float) ((RND.bool() ? 1 : -1) * RND.plusDouble(1e10, 4));

    String s = converter.toStr(value);

    float actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void doubleOne_1() {

    double value = 1.234;

    String s = converter.toStr(value);

    assertThat(s).isEqualTo("U1.234");

    double actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void doubleOne_2() {

    double value = -1.234e100;

    String s = converter.toStr(value);

    assertThat(s).isEqualTo("U-1.234E100");

    double actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void doubleOne_3() {

    double value = 1.234432432e+78;

    String s = converter.toStr(value);

    assertThat(s).isEqualTo("U1.234432432E78");

    double actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void dateOne() throws ParseException {

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    Date value = sdf.parse("1980-01-27 11:12:54.098");

    String s = converter.toStr(value);

    assertThat(s).isEqualTo("D1980-01-27T11:12:54.098");

    Date actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void strOne() {

    String value = "Помидор";

    String s = converter.toStr(value);

    assertThat(s).isEqualTo("SПомидор|");

    String actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void strOne2() {

    String value = RND.str(10) + "|" + RND.str(10);

    String s = converter.toStr(value);

    String actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void boolOne_true() {

    String s = converter.toStr(true);

    assertThat(s).isEqualTo("J");

    boolean actual = converter.fromStr(s);

    assertThat(actual).isTrue();
  }

  @Test
  public void boolOne_false() {

    String s = converter.toStr(false);

    assertThat(s).isEqualTo("K");

    boolean actual = converter.fromStr(s);

    assertThat(actual).isFalse();
  }

  @Test
  public void bdOne_1() {

    BigDecimal value = new BigDecimal("123213213.2315435253453");

    String s = converter.toStr(value);

    assertThat(s).isEqualTo("X123213213.2315435253453");

    BigDecimal actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void bdOne_2() {

    BigDecimal value = new BigDecimal("-123213213e10234");

    String s = converter.toStr(value);

    assertThat(s).isEqualTo("X-1.23213213E+10242");

    BigDecimal actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void bdOne_3() {

    BigDecimal value = new BigDecimal("-1232.13213e-10234");

    String s = converter.toStr(value);

    assertThat(s).isEqualTo("X-1.23213213E-10231");

    BigDecimal actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void nullOne() {

    String s = converter.toStr(null);

    assertThat(s).isEqualTo("N");

    Object actual = converter.fromStr(s);

    assertThat(actual).isNull();

  }

  @Test
  @SuppressWarnings("unchecked")
  public void listOne() {

    List value = new ArrayList();
    value.add("Asd");
    value.add(null);
    value.add(123L);

    String s = converter.toStr(value);

    assertThat(s).isEqualTo("P[SAsd|NL123]");

    List actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void mapOne() {

    Map value = new HashMap();
    value.put("Asd", "dsa");
    value.put("WOW", 123L);
    value.put(111L, null);
    value.put(11, "a|sd");

    String s = converter.toStr(value);

    Map actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);

  }

  @Test
  @SuppressWarnings("unchecked")
  public void setOne() {

    Set value = new HashSet();
    value.add("Asd");
    value.add(null);
    value.add(123L);

    String s = converter.toStr(value);

    Set actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void intArray() {
    int[] value = new int[]{1, -1, 0, 123243254, -324344543};

    String s = converter.toStr(value);

    assertThat(s).isEqualTo("A5I[I1I-1I0I123243254I-324344543]");

    int[] actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void longArray() {
    long[] value = new long[]{1L, 0, -1L, -10_000_000_001L, 10_007_000_001L,};

    String s = converter.toStr(value);

    assertThat(s).isEqualTo("A5L[L1L0L-1L-10000000001L10007000001]");

    long[] actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void doubleArray() {
    double[] value = new double[]{123.7, -12.34e-100, 34.3432e+234, 0};

    String s = converter.toStr(value);

    assertThat(s).isEqualTo("A4U[U123.7U-1.234E-99U3.43432E235U0.0]");

    double[] actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void byteArray() {
    byte[] value = new byte[]{(byte) 17, (byte) -11, (byte) 120, (byte) -123,};

    String s = converter.toStr(value);

    assertThat(s).isEqualTo("bEfV4hQ==|");

    byte[] actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void byteArray_BIG() {
    byte[] value = RND.byteArray(36);

    String s = converter.toStr(value);

    System.out.println("54g325hgv4 :: serialized byte array : `" + s + "`");

    byte[] actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void charArray() {
    char[] value = new char[]{'Ж', 'ж', 'I', 'j', 'u', '1'};

    String s = converter.toStr(value);

    assertThat(s).isEqualTo("cЖжIju1|");

    char[] actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void charArray2() {
    char[] value = new char[]{'Ж', 'ж', 'I', 'j', 'u', '1', '|', 'Q'};

    String s = converter.toStr(value);

    assertThat(s).isEqualTo("cЖжIju1\\|Q|");

    char[] actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void charArray3() {
    char[] value = new char[]{'Ж', 'ж', 'I', '\\', 'j', 'u', '1', '|', 'Q'};

    String s = converter.toStr(value);

    assertThat(s).isEqualTo("cЖжI\\\\ju1\\|Q|");

    char[] actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void floatArray() {
    float[] value = new float[]{123.7f, 11.2f, -1.7e-19f};

    String s = converter.toStr(value);

    assertThat(s).isEqualTo("A3F[F123.7F11.2F-1.7E-19]");

    float[] actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void shortArray() {
    short[] value = new short[]{(short) RND.plusInt(10_000_000),
      (short) RND.plusInt(10_000_000),
      (short) RND.plusInt(10_000_000),};

    String s = converter.toStr(value);

    short[] actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void strArray() {
    String[] value = new String[]{"str1", "Жара", "QQQ", null, "Минус|Плюс"};

    String s = converter.toStr(value);

    //noinspection SpellCheckingInspection
    assertThat(s).isEqualTo("A5S[Sstr1|SЖара|SQQQ|NSМинус\\|Плюс|]");

    String[] actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  public void objectArray() {
    Object[] value = new Object[]{"AЖ1_&", 378, null, 123.654};

    String s = converter.toStr(value);

    assertThat(s).isEqualTo("A4Q[SAЖ1_&|I378NU123.654]");

    Object[] actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void listArray() {
    List[] value = new List[]{new ArrayList(), null, null, new ArrayList()};
    value[0].add(RND.str(10));

    String s = converter.toStr(value);

    List[] actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void mapArray() {
    Map[] value = new Map[]{new HashMap(), null, null, new HashMap()};
    value[0].put(RND.str(10), RND.str(10));

    String s = converter.toStr(value);

    Map[] actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void setArray() {
    Set[] value = new Set[]{new HashSet(), null, null, new HashSet()};
    value[0].add(RND.str(10));

    String s = converter.toStr(value);

    Set[] actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(value);
  }

  public enum TestEnum {
    HI, BI
  }

  public enum TestEnumAbstract {
    WOW {
      @Override
      String saySome() {
        return "some";
      }
    },

    STORY {
      @Override
      String saySome() {
        return "xxx";
      }
    },

    ;

    abstract String saySome();
  }

  public static class ForTest {
    public TestEnum testEnum;

    @Override
    public String toString() {
      return "ForTest{" +
        "testEnum=" + testEnum +
        '}';
    }
  }

  @Test
  public void setEnum() {
    ForTest source = new ForTest();
    source.testEnum = TestEnum.BI;

    String str = converter.toStr(source);

    ForTest actual = converter.fromStr(str);

    assertThat(actual.toString()).isEqualTo(source.toString());
  }

  @Test
  public void enumTest() {
    String s = converter.toStr(TestEnum.HI);

    assertThat(s).isEqualTo("QTestEnum{HI}");

    Object actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(TestEnum.HI);
  }

  @Test
  public void enumWithAbstract() {

    String s = converter.toStr(TestEnumAbstract.WOW);

    assertThat(s).isEqualTo("QTestEnumAbstract{WOW}");

    Object actual = converter.fromStr(s);

    assertThat(actual).isEqualTo(TestEnumAbstract.WOW);

    String someSaid = ((TestEnumAbstract) actual).saySome();
    assertThat(someSaid).isEqualTo("some");

  }

  @Test
  public void enumWithAbstract_onlyParse() {
    Object actual = converter.fromStr("QTestEnumAbstract{STORY}");

    assertThat(actual).isEqualTo(TestEnumAbstract.STORY);

    String someSaid = ((TestEnumAbstract) actual).saySome();
    assertThat(someSaid).isEqualTo("xxx");
  }

  static class CustomField {
    public final long wow;

    public CustomField(long wow) {
      this.wow = wow;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CustomField that = (CustomField) o;
      return wow == that.wow;
    }

    @Override
    public int hashCode() {
      return Objects.hash(wow);
    }
  }

  public static class Hello {
    public CustomField field1;
    public CustomField field2;
    public String hello;
  }

  @Test
  public void customClassManager() {

    StrConverterSimple converter = new StrConverterSimple();

    converter.convertRegistry().register(
      CustomClassManager
        .of(CustomField.class, "F")
        .addOnlyGetter("x", source -> ((CustomField) source).wow)
        .setClassInstantiation((workingClass, acceptorMap, nameValueList) ->
          new CustomField((Long) nameValueList.getValue("x")))
    );

    converter.convertRegistry().register(Hello.class);

    Hello hello = new Hello();
    hello.hello = "Привет";
    hello.field1 = new CustomField(3456L);
    hello.field2 = new CustomField(1000009L);

    String str = converter.toStr(hello);

    System.out.println("5v4c6vg5 :: str = " + str);

    Hello actual = converter.fromStr(str);

    assertThat(actual.field1).isEqualTo(hello.field1);
    assertThat(actual.field2).isEqualTo(hello.field2);
    assertThat(actual.hello).isEqualTo(hello.hello);

  }

  public static class ManyFieldClass {
    public String fieldStr;
    public Date fieldDate;
    public BigDecimal fieldBD;
    public Boolean fieldBool1;
    public Integer fieldInt1;
    public Long fieldLong1;
    public Byte fieldByte1;
    public Short fieldShort1;
    public Double fieldDouble1;
    public Float fieldFloat1;
    public boolean fieldBool2;
    public int fieldInt2;
    public long fieldLong2;
    public byte fieldByte2;
    public short fieldShort2;
    public double fieldDouble2;
    public float fieldFloat2;
  }

  @Test
  public void test_ManyFieldClass__allNulls() {

    ManyFieldClass object = new ManyFieldClass();

    String s = converter.toStr(object);

    System.out.println("54ghv26c :: s = " + s);

    ManyFieldClass actual = converter.fromStr(s);

    assertThat(actual.fieldStr).isNull();
    assertThat(actual.fieldDate).isNull();
    assertThat(actual.fieldBD).isNull();
    assertThat(actual.fieldBool1).isNull();
    assertThat(actual.fieldInt1).isNull();
    assertThat(actual.fieldLong1).isNull();
    assertThat(actual.fieldByte1).isNull();
    assertThat(actual.fieldShort1).isNull();
    assertThat(actual.fieldDouble1).isNull();
    assertThat(actual.fieldFloat1).isNull();
    assertThat(actual.fieldBool2).isFalse();
    assertThat(actual.fieldInt2).isZero();
    assertThat(actual.fieldLong2).isZero();
    assertThat(actual.fieldByte2).isZero();
    assertThat(actual.fieldShort2).isZero();
    assertThat(actual.fieldDouble2).isZero();
    assertThat(actual.fieldFloat2).isZero();

  }

  @Test
  public void test_ManyFieldClass() {

    ManyFieldClass object = new ManyFieldClass();
    object.fieldStr = RND.str(10);
    object.fieldDate = RND.dateDays(-100, 0);
    object.fieldBD = RND.bd(1_000_000_000_000L, 7);

    object.fieldBool1 = RND.bool();
    object.fieldInt1 = RND.plusInt(1_000_000_000);
    object.fieldLong1 = RND.plusLong(1_000_000_000_000L);
    object.fieldByte1 = (byte) RND.plusInt(1_000);
    object.fieldShort1 = (short) RND.plusInt(1_000_000_000);
    object.fieldDouble1 = RND.plusDouble(2e7, 7) - 1e7;
    object.fieldFloat1 = (float) (RND.plusDouble(2e7, 7) - 1e7);

    object.fieldBool2 = RND.bool();
    object.fieldInt2 = RND.plusInt(1_000_000_000);
    object.fieldLong2 = RND.plusLong(1_000_000_000_000L);
    object.fieldByte2 = (byte) RND.plusInt(1_000);
    object.fieldShort2 = (short) RND.plusInt(1_000_000_000);
    object.fieldDouble2 = RND.plusDouble(2e7, 7) - 1e7;
    object.fieldFloat2 = (float) (RND.plusDouble(2e7, 7) - 1e7);

    String s = converter.toStr(object);

    System.out.println("54jv326vgh :: s = " + s);

    ManyFieldClass actual = converter.fromStr(s);

    assertThat(actual.fieldStr).isEqualTo(object.fieldStr);
    assertThat(actual.fieldDate).isEqualTo(object.fieldDate);
    assertThat(actual.fieldBD).isEqualTo(object.fieldBD);
    assertThat(actual.fieldBool1).isEqualTo(object.fieldBool1);
    assertThat(actual.fieldInt1).isEqualTo(object.fieldInt1);
    assertThat(actual.fieldLong1).isEqualTo(object.fieldLong1);
    assertThat(actual.fieldByte1).isEqualTo(object.fieldByte1);
    assertThat(actual.fieldShort1).isEqualTo(object.fieldShort1);
    assertThat(actual.fieldDouble1).isEqualTo(object.fieldDouble1);
    assertThat(actual.fieldFloat1).isEqualTo(object.fieldFloat1);
    assertThat(actual.fieldBool2).isEqualTo(object.fieldBool2);
    assertThat(actual.fieldInt2).isEqualTo(object.fieldInt2);
    assertThat(actual.fieldLong2).isEqualTo(object.fieldLong2);
    assertThat(actual.fieldByte2).isEqualTo(object.fieldByte2);
    assertThat(actual.fieldShort2).isEqualTo(object.fieldShort2);
    assertThat(actual.fieldDouble2).isEqualTo(object.fieldDouble2);
    assertThat(actual.fieldFloat2).isEqualTo(object.fieldFloat2);

  }

  public static class OnlyFinalFieldsClass {
    public final String field1;
    public final String field2;
    public final String field3;

    @java.beans.ConstructorProperties({"field1", "field2", "field3"})
    public OnlyFinalFieldsClass(String field1, String field2, String field3) {
      this.field1 = field1;
      this.field2 = field2;
      this.field3 = field3;
    }
  }

  @Test
  public void test_OnlyFinalFieldsClass() {

    OnlyFinalFieldsClass object = new OnlyFinalFieldsClass(RND.str(10), RND.str(10), RND.str(10));

    String s = converter.toStr(object);

    System.out.println("54jv326vgh :: s = " + s);

    OnlyFinalFieldsClass actual = converter.fromStr(s);

    assertThat(actual).isNotNull();
    assertThat(actual.field1).isEqualTo(object.field1);
    assertThat(actual.field2).isEqualTo(object.field2);
    assertThat(actual.field3).isEqualTo(object.field3);

  }

  public static class WithFinalFieldsClass {
    public final String finalField1;
    public final String finalField2;
    public final String finalField3;

    public String field4;
    public String field5;
    public String field6;

    @java.beans.ConstructorProperties({"finalField1", "finalField2", "finalField3"})
    public WithFinalFieldsClass(String finalField1, String finalField2, String finalField3) {
      this.finalField1 = finalField1;
      this.finalField2 = finalField2;
      this.finalField3 = finalField3;
    }
  }

  @Test
  public void test_WithFinalFieldsClass() {
    WithFinalFieldsClass object = new WithFinalFieldsClass(RND.str(10), RND.str(10), RND.str(10));
    object.field4 = RND.str(10);
    object.field5 = RND.str(10);
    object.field6 = RND.str(10);

    String s = converter.toStr(object);

    System.out.println("6c54f72f7c :: s = " + s);

    WithFinalFieldsClass actual = converter.fromStr(s);

    assertThat(actual).isNotNull();
    assertThat(actual.finalField1).isEqualTo(object.finalField1);
    assertThat(actual.finalField2).isEqualTo(object.finalField2);
    assertThat(actual.finalField3).isEqualTo(object.finalField3);
    assertThat(actual.field4).isEqualTo(object.field4);
    assertThat(actual.field5).isEqualTo(object.field5);
    assertThat(actual.field6).isEqualTo(object.field6);
  }

  public static class ClassWithBoolean {
    public String str;
    public Boolean boolField;
  }

  @Test
  public void classWithBoxedBoolean() {
    ClassWithBoolean v = new ClassWithBoolean();
    v.str = "jЮgАHдlЖws";
    v.boolField = null;

    String s = converter.toStr(v);

    assertThat(s).isEqualTo("QClassWithBoolean{str=SjЮgАHдlЖws|}");

    ClassWithBoolean actual = converter.fromStr(s);

    assertThat(actual.str).isEqualTo(v.str);
    assertThat(actual.boolField).isNull();
  }

}
