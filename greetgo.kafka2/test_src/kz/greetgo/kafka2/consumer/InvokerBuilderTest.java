package kz.greetgo.kafka2.consumer;

import kz.greetgo.kafka2.consumer.annotations.Author;
import kz.greetgo.kafka2.consumer.annotations.ConsumerName;
import kz.greetgo.kafka2.consumer.annotations.KafkaCommitOn;
import kz.greetgo.kafka2.consumer.annotations.Offset;
import kz.greetgo.kafka2.consumer.annotations.Partition;
import kz.greetgo.kafka2.consumer.annotations.Timestamp;
import kz.greetgo.kafka2.consumer.annotations.Topic;
import kz.greetgo.kafka2.model.Box;
import kz.greetgo.util.RND;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static kz.greetgo.kafka2.consumer.RecordUtil.recordOf;
import static kz.greetgo.kafka2.consumer.RecordUtil.recordWithOffset;
import static kz.greetgo.kafka2.consumer.RecordUtil.recordWithPartition;
import static kz.greetgo.kafka2.consumer.RecordUtil.recordWithTimestamp;
import static kz.greetgo.kafka2.consumer.RecordUtil.recordsOf;
import static kz.greetgo.kafka2.util_for_tests.ReflectionUtil.findMethod;
import static org.fest.assertions.api.Assertions.assertThat;

public class InvokerBuilderTest {

  static class C1 {

    final Set<String> authors = new HashSet<>();

    @Topic({"test1", "test2"})
    @SuppressWarnings("unused")
    public void method1(Box box) {
      authors.add(box.author);
    }

  }

  @Test
  public void build_invoke__box_filteringByTopic() {
    C1 c1 = new C1();
    Method method = findMethod(c1, "method1");

    Box box1 = new Box();
    box1.author = RND.str(10);
    Box box2 = new Box();
    box2.author = RND.str(10);
    Box box3 = new Box();
    box3.author = RND.str(10);

    ConsumerRecord<byte[], Box> record1 = recordOf("test1", new byte[0], box1);
    ConsumerRecord<byte[], Box> record2 = recordOf("test2", new byte[0], box2);
    ConsumerRecord<byte[], Box> record3 = recordOf("test3", new byte[0], box3);

    ConsumerRecords<byte[], Box> records = recordsOf(asList(record1, record2, record3));

    //
    //
    boolean toCommit = new InvokerBuilder(c1, method, null).build().invoke(records);
    //
    //

    assertThat(c1.authors).contains(box1.author);
    assertThat(c1.authors).contains(box2.author);
    assertThat(c1.authors).hasSize(2);
    assertThat(toCommit).isTrue();
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
  public void build_invoke__innerType_filteringByType() {
    C2 c2 = new C2();
    Method method = findMethod(c2, "method1");

    C2_Model1 model1 = new C2_Model1();
    C2_Model2 model2 = new C2_Model2();

    Box box1 = new Box();
    box1.body = model1;
    Box box2 = new Box();
    box2.body = model2;

    ConsumerRecord<byte[], Box> record1 = recordOf("test1", new byte[0], box1);
    ConsumerRecord<byte[], Box> record2 = recordOf("test1", new byte[0], box2);

    ConsumerRecords<byte[], Box> records = recordsOf(asList(record1, record2));

    //
    //
    boolean toCommit = new InvokerBuilder(c2, method, null).build().invoke(records);
    //
    //

    assertThat(c2.model).isSameAs(model1);
    assertThat(toCommit).isTrue();
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
  public void build_invoke__author() {
    C3 c3 = new C3();
    Method method = findMethod(c3, "method1");

    Box box = new Box();
    box.author = RND.str(10);

    ConsumerRecord<byte[], Box> record1 = recordOf("test1", new byte[0], box);

    ConsumerRecords<byte[], Box> records = recordsOf(singletonList(record1));

    //
    //
    boolean toCommit = new InvokerBuilder(c3, method, null).build().invoke(records);
    //
    //

    assertThat(c3.author).isSameAs(box.author);
    assertThat(toCommit).isTrue();
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
  public void build_invoke__timestamp() {
    C4 c4 = new C4();
    Method method = findMethod(c4, "method1");

    Box box = new Box();

    Date timestamp = RND.dateDays(-10_000, -1);

    ConsumerRecord<byte[], Box> record1 = recordWithTimestamp("test1", timestamp, box);

    ConsumerRecords<byte[], Box> records = recordsOf(singletonList(record1));

    //
    //
    boolean toCommit = new InvokerBuilder(c4, method, null).build().invoke(records);
    //
    //

    assertThat(c4.timestamp).isEqualTo(timestamp.getTime());
    assertThat(c4.timestampDate).isEqualTo(timestamp);
    assertThat(toCommit).isTrue();
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
  public void build_invoke__ignoreConsumer() {
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
    boolean toCommit = new InvokerBuilder(c5, method, null).build().invoke(records);
    //
    //

    assertThat(c5.authors).contains(box3.author);
    assertThat(c5.authors).contains(box4.author);
    assertThat(c5.authors).hasSize(2);
    assertThat(toCommit).isTrue();

  }

  static class C6 {

    String errorMessage;

    @Topic("test1")
    @SuppressWarnings("unused")
    public void method1(Box box) {
      throw new RuntimeException(errorMessage);
    }

  }

  @Test
  public void build_invoke__returnsFalseBecauseOfExceptionInMethod() {
    C6 c6 = new C6();
    Method method = findMethod(c6, "method1");

    Box box = new Box();

    c6.errorMessage = RND.str(10);

    ConsumerRecord<byte[], Box> record = recordOf("test1", new byte[0], box);
    ConsumerRecords<byte[], Box> records = recordsOf(singletonList(record));

    TestConsumerLogger testConsumerLogger = new TestConsumerLogger();

    //
    //
    boolean toCommit = new InvokerBuilder(c6, method, testConsumerLogger).build().invoke(records);
    //
    //

    assertThat(testConsumerLogger.errorList).hasSize(1);
    assertThat(testConsumerLogger.errorList.get(0).getMessage()).isEqualTo(c6.errorMessage);

    assertThat(toCommit).isFalse();
  }

  static class Error1 extends RuntimeException {}

  static class Error2 extends RuntimeException {}

  static class C7 {

    @Topic("test1")
    @SuppressWarnings("unused")
    @KafkaCommitOn({Error1.class, Error2.class})
    public void method1(Box box) {
      throw (RuntimeException) box.body;
    }

  }

  @Test
  public void build_invoke__returnsTrueBecauseOfAnnotation_KafkaCommitOn() {
    C7 c7 = new C7();
    Method method = findMethod(c7, "method1");

    Box box1 = new Box();
    box1.body = new Error1();
    Box box2 = new Box();
    box2.body = new Error2();

    ConsumerRecord<byte[], Box> record1 = recordOf("test1", new byte[0], box1);
    ConsumerRecord<byte[], Box> record2 = recordOf("test1", new byte[0], box2);

    ConsumerRecords<byte[], Box> records = recordsOf(asList(record1, record2));

    TestConsumerLogger testConsumerLogger = new TestConsumerLogger();

    //
    //
    boolean toCommit = new InvokerBuilder(c7, method, testConsumerLogger).build().invoke(records);
    //
    //

    assertThat(testConsumerLogger.errorList).hasSize(2);
    assertThat(testConsumerLogger.errorList.get(0)).isSameAs((Error1) box1.body);
    assertThat(testConsumerLogger.errorList.get(1)).isSameAs((Error2) box2.body);
    assertThat(toCommit).isTrue();
  }

  static class C8 {

    Integer partition = null;

    @Topic("test1")
    @SuppressWarnings("unused")
    public void method1(Box box, @Partition int partition) {
      this.partition = partition;
    }

  }

  @Test
  public void build_invoke__partition() {
    C8 c8 = new C8();
    Method method = findMethod(c8, "method1");

    Box box = new Box();

    int partition = RND.plusInt(1_000_000);

    ConsumerRecord<byte[], Box> record1 = recordWithPartition("test1", partition, box);

    ConsumerRecords<byte[], Box> records = recordsOf(singletonList(record1));

    //
    //
    boolean toCommit = new InvokerBuilder(c8, method, null).build().invoke(records);
    //
    //

    assertThat(c8.partition).isEqualTo(partition);
    assertThat(toCommit).isTrue();
  }


  static class C9 {

    Long offset = null;

    @Topic("test1")
    @SuppressWarnings("unused")
    public void method1(Box box, @Offset long offset) {
      this.offset = offset;
    }

  }

  @Test
  public void build_invoke__offset() {
    C9 c9 = new C9();
    Method method = findMethod(c9, "method1");

    Box box = new Box();

    long offset = RND.plusLong(1_000_000_000_000L);

    ConsumerRecord<byte[], Box> record1 = recordWithOffset("test1", offset, box);

    ConsumerRecords<byte[], Box> records = recordsOf(singletonList(record1));

    //
    //
    boolean toCommit = new InvokerBuilder(c9, method, null).build().invoke(records);
    //
    //

    assertThat(c9.offset).isEqualTo(offset);
    assertThat(toCommit).isTrue();
  }
}
