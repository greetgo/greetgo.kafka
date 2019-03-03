package kz.greetgo.kafka2.consumer;

import kz.greetgo.kafka2.consumer.annotations.Author;
import kz.greetgo.kafka2.consumer.annotations.ConsumerName;
import kz.greetgo.kafka2.consumer.annotations.Timestamp;
import kz.greetgo.kafka2.consumer.annotations.Topic;
import kz.greetgo.kafka2.model.Box;
import kz.greetgo.util.RND;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static kz.greetgo.kafka2.consumer.RecordUtil.recordOf;
import static kz.greetgo.kafka2.consumer.RecordUtil.recordWithTimestamp;
import static kz.greetgo.kafka2.consumer.RecordUtil.recordsOf;
import static kz.greetgo.kafka2.util.ReflectionUtil.findMethod;
import static org.fest.assertions.api.Assertions.assertThat;

public class InvokerBuilderTest {

  static class C1 {

    final List<Box> boxes = new ArrayList<>();

    @Topic({"test1", "test2"})
    @SuppressWarnings("unused")
    public void method1(Box box) {
      boxes.add(box);
    }

  }

  @Test
  public void build_box_filteringByTopic() {
    C1 c1 = new C1();
    Method method = findMethod(c1, "method1");

    Box box1 = new Box();
    Box box2 = new Box();
    Box box3 = new Box();

    ConsumerRecord<byte[], Box> record1 = recordOf("test1", new byte[0], box1);
    ConsumerRecord<byte[], Box> record2 = recordOf("test2", new byte[0], box2);
    ConsumerRecord<byte[], Box> record3 = recordOf("test3", new byte[0], box3);

    ConsumerRecords<byte[], Box> records = recordsOf(asList(record1, record2, record3));

    //
    //
    new InvokerBuilder(c1, method).build().invoke(records);
    //
    //

    assertThat(c1.boxes).hasSize(2);
    assertThat(c1.boxes.get(0)).isSameAs(box1);
    assertThat(c1.boxes.get(1)).isSameAs(box2);
  }

  static class C2_Model1 {}

  static class C2_Model2 {}

  static class C2 {

    C2_Model1 model;

    @Topic("test1")
    @SuppressWarnings("unused")
    public void method1(C2_Model1 model) {
      this.model = model;
    }

  }

  @Test
  public void build_innerType_filteringByType() {
    C2 c2 = new C2();
    Method method = findMethod(c2, "method1");

    C2_Model1 model1 = new C2_Model1();
    C2_Model2 model2 = new C2_Model2();

    Box box1 = new Box(model1);
    Box box2 = new Box(model2);

    ConsumerRecord<byte[], Box> record1 = recordOf("test1", new byte[0], box1);
    ConsumerRecord<byte[], Box> record2 = recordOf("test2", new byte[0], box2);

    ConsumerRecords<byte[], Box> records = recordsOf(asList(record1, record2));

    //
    //
    new InvokerBuilder(c2, method).build().invoke(records);
    //
    //

    assertThat(c2.model).isSameAs(model1);
  }

  static class C3 {

    String author;

    @Topic("test1")
    @SuppressWarnings("unused")
    public void method1(Box box, @Author String author) {
      this.author = author;
    }

  }

  @Test
  public void build_author() {
    C3 c3 = new C3();
    Method method = findMethod(c3, "method1");

    Box box = new Box();
    box.author = RND.str(10);

    ConsumerRecord<byte[], Box> record1 = recordOf("test1", new byte[0], box);

    ConsumerRecords<byte[], Box> records = recordsOf(singletonList(record1));

    //
    //
    new InvokerBuilder(c3, method).build().invoke(records);
    //
    //

    assertThat(c3.author).isSameAs(box.author);
  }

  static class C4 {
    Long timestamp = null;
    Date timestampDate = null;

    @Topic("test1")
    @SuppressWarnings("unused")
    public void method1(Box box, @Timestamp long timestamp, @Timestamp Date timestampDate) {
      this.timestamp = timestamp;
      this.timestampDate = timestampDate;
    }

  }

  @Test
  public void build_timestamp() {
    C4 c4 = new C4();
    Method method = findMethod(c4, "method1");

    Box box = new Box();

    Date timestamp = RND.dateDays(-10_000, -1);

    ConsumerRecord<byte[], Box> record1 = recordWithTimestamp("test1", timestamp, box);

    ConsumerRecords<byte[], Box> records = recordsOf(singletonList(record1));

    //
    //
    new InvokerBuilder(c4, method).build().invoke(records);
    //
    //

    assertThat(c4.timestamp).isEqualTo(timestamp.getTime());
    assertThat(c4.timestampDate).isEqualTo(timestamp);
  }

  static class C5 {

    final Set<String> authors = new HashSet<>();

    @Topic("test1")
    @SuppressWarnings("unused")
    @ConsumerName("coolConsumer")
    public void method1(Box box) {
      authors.add(box.author);
    }

  }

  @Test
  public void build_ignoreConsumer() {
    C5 c5 = new C5();
    Method method = findMethod(c5, "method1");

    Box box1 = new Box();
    box1.ignorableConsumers = asList("coolConsumer", "wow");
    box1.author = RND.str(10);

    Box box2 = new Box();
    box2.ignorableConsumers = asList("asd1", "coolConsumer");
    box2.author = RND.str(10);

    Box box3 = new Box();
    box3.ignorableConsumers = asList("asd1", "asd2");
    box3.author = RND.str(10);

    Box box4 = new Box();
    box4.ignorableConsumers = null;
    box4.author = RND.str(10);

    ConsumerRecord<byte[], Box> record1 = recordOf("test1", new byte[0], box1);
    ConsumerRecord<byte[], Box> record2 = recordOf("test1", new byte[0], box2);
    ConsumerRecord<byte[], Box> record3 = recordOf("test1", new byte[0], box3);
    ConsumerRecord<byte[], Box> record4 = recordOf("test1", new byte[0], box4);

    ConsumerRecords<byte[], Box> records = recordsOf(asList(record1, record2, record3, record4));

    //
    //
    new InvokerBuilder(c5, method).build().invoke(records);
    //
    //

    assertThat(c5.authors).contains(box1.author);
    assertThat(c5.authors).contains(box2.author);
    assertThat(c5.authors).hasSize(2);
  }
}
