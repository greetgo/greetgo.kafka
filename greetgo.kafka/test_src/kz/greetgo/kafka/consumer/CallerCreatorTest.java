package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.core.Box;
import kz.greetgo.kafka.core.BoxRecord;
import kz.greetgo.kafka.core.Head;
import kz.greetgo.kafka.core.RecordPlace;
import kz.greetgo.util.RND;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;

public class CallerCreatorTest {

  private static Method getTestingMethod(Class<?> aClass) {
    for (Method method : aClass.getDeclaredMethods()) {
      if ("testingMethod".equals(method.getName())) return method;
    }
    throw new IllegalArgumentException("No testing method in " + aClass);
  }

  private static List<BoxRecord> toBoxRecordList(List<Box> boxList) {
    return boxList.stream().map(CallerCreatorTest::toBoxRecord).collect(Collectors.toList());
  }

  private static BoxRecord toRecord(Box box, String topic, int partition, long offset) {
    return new BoxRecord() {
      @Override
      public Box box() {
        return box;
      }

      @Override
      public RecordPlace place() {
        return new RecordPlace(topic, partition, offset);
      }
    };
  }

  private static BoxRecord toBoxRecord(Box box) {
    return new BoxRecord() {
      @Override
      public Box box() {
        return box;
      }

      @Override
      public RecordPlace place() {
        throw new UnsupportedOperationException();
      }
    };
  }

  private static class Testing1 {
    public List<Box> listBox = null;

    @SuppressWarnings("unused")
    public void testingMethod(List<Box> list) {
      if (listBox != null) throw new IllegalStateException();
      listBox = list;
    }
  }

  @Test
  public void test_listOfBoxes() throws Exception {


    Testing1 testing = new Testing1();

    Method method = getTestingMethod(testing.getClass());

    //
    //
    Caller caller = CallerCreator.create(testing, method);
    //
    //

    assertThat(caller).isNotNull();

    String tmpStr = RND.str(10);

    List<Box> list = new ArrayList<>();
    {
      Box box = new Box();
      box.head = new Head();
      box.head.a = RND.str(10);
      box.body = tmpStr;
      list.add(box);
    }

    caller.call(toBoxRecordList(list));

    assertThat(testing.listBox).isNotNull();
    assertThat(testing.listBox.get(0).head.a).isEqualTo(list.get(0).head.a);
    assertThat(testing.listBox.get(0).body).isEqualTo(tmpStr);
  }

  private static class Testing2 {
    public List<Object> listObject = null;

    @SuppressWarnings("unused")
    public void testingMethod(List<Object> list) {
      if (listObject != null) throw new IllegalStateException();
      listObject = list;
    }
  }

  @Test
  public void test_listOfObjects_hasDifferentIncludes() throws Exception {

    Testing2 testing = new Testing2();

    Method method = getTestingMethod(testing.getClass());

    //
    //
    Caller caller = CallerCreator.create(testing, method);
    //
    //

    assertThat(caller).isNotNull();

    List<Box> list = new ArrayList<>();
    list.add(boxWithBody("a"));
    list.add(boxWithBody(asList("b", "c")));
    list.add(boxWithBody(asList(asList("d", "e"), "f")));

    caller.call(toBoxRecordList(list));

    assertThat(testing.listObject).containsExactly("a", "b", "c", "d", "e", "f");
  }

  private static Box boxWithBody(Object body) {
    Box box = new Box();
    box.head = new Head();
    box.head.a = RND.str(10);
    box.body = body;
    return box;
  }

  @Test
  public void test_listOfObjects() throws Exception {

    Testing2 testing = new Testing2();

    Method method = getTestingMethod(testing.getClass());

    //
    //
    Caller caller = CallerCreator.create(testing, method);
    //
    //

    assertThat(caller).isNotNull();

    String tmpStr = RND.str(10);

    List<Box> list = new ArrayList<>();
    {
      Box box = new Box();
      box.head = new Head();
      box.head.a = RND.str(10);
      box.body = tmpStr;
      list.add(box);
    }

    caller.call(toBoxRecordList(list));

    assertThat(testing.listObject).isNotNull();
    assertThat(testing.listObject.get(0)).isEqualTo(tmpStr);
  }

  private static class Testing3 {

    public final List<Box> box_list = new ArrayList<>();

    @SuppressWarnings("unused")
    public void testingMethod(Box box) {
      box_list.add(box);
    }

  }

  @Test
  public void test_box() throws Exception {

    Testing3 testing = new Testing3();

    Method method = getTestingMethod(testing.getClass());

    //
    //
    Caller caller = CallerCreator.create(testing, method);
    //
    //

    assertThat(caller).isNotNull();

    String tmpStr1 = RND.str(10);
    String tmpStr2 = RND.str(10);

    List<Box> list = new ArrayList<>();
    {
      Box box = new Box();
      box.head = new Head();
      box.head.a = RND.str(10);
      box.body = tmpStr1;
      list.add(box);
    }
    {
      Box box = new Box();
      box.head = new Head();
      box.head.a = RND.str(10);
      box.body = tmpStr2;
      list.add(box);
    }

    caller.call(toBoxRecordList(list));

    assertThat(testing.box_list).hasSize(2);
    assertThat(testing.box_list.get(0).body).isEqualTo(tmpStr1);
    assertThat(testing.box_list.get(0).head.a).isEqualTo(list.get(0).head.a);
    assertThat(testing.box_list.get(1).body).isEqualTo(tmpStr2);
    assertThat(testing.box_list.get(1).head.a).isEqualTo(list.get(1).head.a);
  }


  private static class Testing4 {

    public final List<Object> object_list = new ArrayList<>();

    @SuppressWarnings("unused")
    public void testingMethod(Object object) {
      object_list.add(object);
    }

  }

  @Test
  public void test_object() throws Exception {

    Testing4 testing = new Testing4();

    Method method = getTestingMethod(testing.getClass());

    //
    //
    Caller caller = CallerCreator.create(testing, method);
    //
    //

    assertThat(caller).isNotNull();

    String tmpStr1 = RND.str(10);
    String tmpStr2 = RND.str(10);

    List<Box> list = new ArrayList<>();
    {
      Box box = new Box();
      box.head = new Head();
      box.head.a = RND.str(10);
      box.body = tmpStr1;
      list.add(box);
    }
    {
      Box box = new Box();
      box.head = new Head();
      box.head.a = RND.str(10);
      box.body = tmpStr2;
      list.add(box);
    }

    caller.call(toBoxRecordList(list));

    assertThat(testing.object_list).hasSize(2);
    assertThat(testing.object_list.get(0)).isEqualTo(tmpStr1);
    assertThat(testing.object_list.get(1)).isEqualTo(tmpStr2);
  }

  private static class Testing5 {
    public final List<Object> objectHead_objectList = new ArrayList<>();
    public final List<Head> objectHead_headList = new ArrayList<>();

    @SuppressWarnings("unused")
    public void testingMethod(Object object, Head head) {
      objectHead_objectList.add(object);
      objectHead_headList.add(head);
    }
  }

  @Test
  public void test_objectHead() throws Exception {

    Testing5 testing = new Testing5();

    Method method = getTestingMethod(testing.getClass());

    //
    //
    Caller caller = CallerCreator.create(testing, method);
    //
    //

    assertThat(caller).isNotNull();

    String tmpStr1 = RND.str(10);
    String tmpStr2 = RND.str(10);

    List<Box> list = new ArrayList<>();
    {
      Box box = new Box();
      box.head = new Head();
      box.head.a = RND.str(10);
      box.body = tmpStr1;
      list.add(box);
    }
    {
      Box box = new Box();
      box.head = new Head();
      box.head.a = RND.str(10);
      box.body = tmpStr2;
      list.add(box);
    }

    caller.call(toBoxRecordList(list));

    assertThat(testing.objectHead_objectList).hasSize(2);
    assertThat(testing.objectHead_objectList.get(0)).isEqualTo(tmpStr1);
    assertThat(testing.objectHead_objectList.get(1)).isEqualTo(tmpStr2);

    assertThat(testing.objectHead_headList).hasSize(2);
    assertThat(testing.objectHead_headList.get(0).a).isEqualTo(list.get(0).head.a);
    assertThat(testing.objectHead_headList.get(1).a).isEqualTo(list.get(1).head.a);
  }

  private static class Testing6 {
    public List<BoxRecord> boxRecordList = null;

    @SuppressWarnings("unused")
    public void testingMethod(List<BoxRecord> boxRecordList) {
      if (this.boxRecordList != null) throw new IllegalStateException();
      this.boxRecordList = boxRecordList;
    }
  }

  @Test
  public void test_listOfBoxRecords() throws Exception {

    Testing6 testing = new Testing6();


    Method method = getTestingMethod(testing.getClass());

    //
    //
    Caller caller = CallerCreator.create(testing, method);
    //
    //

    assertThat(caller).isNotNull();

    String tmpStr1 = RND.str(10);
    String tmpStr2 = RND.str(10);

    String topic1 = RND.str(10);
    String topic2 = RND.str(10);

    int partition1 = RND.plusInt(1_000_000);
    int partition2 = RND.plusInt(1_000_000);

    long offset1 = RND.plusLong(1_000_000_000);
    long offset2 = RND.plusLong(1_000_000_000);

    String author1 = RND.str(10);
    String author2 = RND.str(10);

    List<BoxRecord> list = new ArrayList<>();
    {
      Box box = new Box();
      box.head = new Head();
      box.head.a = author1;
      box.body = tmpStr1;
      list.add(toRecord(box, topic1, partition1, offset1));
    }
    {
      Box box = new Box();
      box.head = new Head();
      box.head.a = author2;
      box.body = tmpStr2;
      list.add(toRecord(box, topic2, partition2, offset2));
    }

    caller.call(list);

    assertThat(testing.boxRecordList).hasSize(2);

    assertThat(testing.boxRecordList.get(0).box().body).isEqualTo(tmpStr1);
    assertThat(testing.boxRecordList.get(0).box().head.a).isEqualTo(author1);
    assertThat(testing.boxRecordList.get(0).place().topic).isEqualTo(topic1);
    assertThat(testing.boxRecordList.get(0).place().partition).isEqualTo(partition1);
    assertThat(testing.boxRecordList.get(0).place().offset).isEqualTo(offset1);

    assertThat(testing.boxRecordList.get(1).box().body).isEqualTo(tmpStr2);
    assertThat(testing.boxRecordList.get(1).box().head.a).isEqualTo(author2);
    assertThat(testing.boxRecordList.get(1).place().topic).isEqualTo(topic2);
    assertThat(testing.boxRecordList.get(1).place().partition).isEqualTo(partition2);
    assertThat(testing.boxRecordList.get(1).place().offset).isEqualTo(offset2);
  }

}