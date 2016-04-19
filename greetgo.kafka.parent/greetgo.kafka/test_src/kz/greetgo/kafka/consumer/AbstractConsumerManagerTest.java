package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.consumer.AbstractConsumerManager.Caller;
import kz.greetgo.kafka.core.Box;
import kz.greetgo.kafka.core.Head;
import kz.greetgo.util.RND;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static org.fest.assertions.api.Assertions.assertThat;

public class AbstractConsumerManagerTest {

  @SuppressWarnings("unused")
  private static class Testing {

    public List<Box> listBox_1 = null;

    public void listBox(List<Box> list) {
      listBox_1 = list;
    }

    public List<Object> listObject_1 = null;

    public void listObject(List<Object> list) {
      listObject_1 = list;
    }

    public final List<Box> box_list = new ArrayList<>();

    public void box(Box box) {
      box_list.add(box);
    }

    public final List<Object> object_list = new ArrayList<>();

    public void object(Object object) {
      object_list.add(object);
    }

    public final List<Object> objectHead_objectList = new ArrayList<>();
    public final List<Head> objectHead_headList = new ArrayList<>();

    public void objectHead(Object object, Head head) {
      objectHead_objectList.add(object);
      objectHead_headList.add(head);
    }
  }

  @Test
  public void createCaller_listBox() throws Exception {

    Method method = Testing.class.getMethod("listBox", List.class);

    Testing testing = new Testing();

    //
    //
    Caller caller = AbstractConsumerManager.createCaller(testing, method);
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

    caller.call(list);

    assertThat(testing.listBox_1).isNotNull();
    assertThat(testing.listBox_1.get(0).head.a).isEqualTo(list.get(0).head.a);
    assertThat(testing.listBox_1.get(0).body).isEqualTo(tmpStr);
  }

  @Test
  public void createCaller_listObject() throws Exception {

    Method method = Testing.class.getMethod("listObject", List.class);

    Testing testing = new Testing();

    //
    //
    Caller caller = AbstractConsumerManager.createCaller(testing, method);
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

    caller.call(list);

    assertThat(testing.listObject_1).isNotNull();
    assertThat(testing.listObject_1.get(0)).isEqualTo(tmpStr);
  }

  @Test
  public void createCaller_box() throws Exception {

    Method method = Testing.class.getMethod("box", Box.class);

    Testing testing = new Testing();

    //
    //
    Caller caller = AbstractConsumerManager.createCaller(testing, method);
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

    caller.call(list);

    assertThat(testing.box_list).hasSize(2);
    assertThat(testing.box_list.get(0).body).isEqualTo(tmpStr1);
    assertThat(testing.box_list.get(0).head.a).isEqualTo(list.get(0).head.a);
    assertThat(testing.box_list.get(1).body).isEqualTo(tmpStr2);
    assertThat(testing.box_list.get(1).head.a).isEqualTo(list.get(1).head.a);
  }

  @Test
  public void createCaller_object() throws Exception {

    Method method = Testing.class.getMethod("object", Object.class);

    Testing testing = new Testing();

    //
    //
    Caller caller = AbstractConsumerManager.createCaller(testing, method);
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

    caller.call(list);

    assertThat(testing.object_list).hasSize(2);
    assertThat(testing.object_list.get(0)).isEqualTo(tmpStr1);
    assertThat(testing.object_list.get(1)).isEqualTo(tmpStr2);
  }

  @Test
  public void createCaller_objectHead() throws Exception {

    Method method = Testing.class.getMethod("objectHead", Object.class, Head.class);

    Testing testing = new Testing();

    //
    //
    Caller caller = AbstractConsumerManager.createCaller(testing, method);
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

    caller.call(list);

    assertThat(testing.objectHead_objectList).hasSize(2);
    assertThat(testing.objectHead_objectList.get(0)).isEqualTo(tmpStr1);
    assertThat(testing.objectHead_objectList.get(1)).isEqualTo(tmpStr2);

    assertThat(testing.objectHead_headList).hasSize(2);
    assertThat(testing.objectHead_headList.get(0).a).isEqualTo(list.get(0).head.a);
    assertThat(testing.objectHead_headList.get(1).a).isEqualTo(list.get(1).head.a);
  }
}