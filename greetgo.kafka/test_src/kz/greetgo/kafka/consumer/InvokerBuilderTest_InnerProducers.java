package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.consumer.annotations.InnerProducerName;
import kz.greetgo.kafka.consumer.annotations.ToTopic;
import kz.greetgo.kafka.consumer.annotations.Topic;
import kz.greetgo.kafka.consumer.test_models.InputModel;
import kz.greetgo.kafka.consumer.test_models.OutputModel;
import kz.greetgo.kafka.core.KafkaReactor;
import kz.greetgo.kafka.errors.AbsentAnnotationToTopicOverInnerProducer;
import kz.greetgo.kafka.model.Box;
import kz.greetgo.kafka.util_for_tests.TestProducerFacade;
import kz.greetgo.util.RND;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static java.util.Arrays.asList;
import static kz.greetgo.kafka.consumer.RecordUtil.recordOf;
import static kz.greetgo.kafka.consumer.RecordUtil.recordsOf;
import static kz.greetgo.kafka.util_for_tests.ReflectionUtil.findMethod;
import static org.fest.assertions.api.Assertions.assertThat;

public class InvokerBuilderTest_InnerProducers {

  static class C1 {

    final List<InputModel> inputModelList = new ArrayList<>();
    final LinkedList<OutputModel> outputModelList = new LinkedList<>();

    @Topic({"test1"})
    @SuppressWarnings("unused")
    public void consumerMethod(InputModel inputModel,
                               @ToTopic("outTest")
                               @InnerProducerName("test_producer")
                                 InnerProducer<OutputModel> producer
    ) {
      inputModelList.add(inputModel);
      producer.send(outputModelList.removeFirst());
    }

  }

  @Test
  public void build_invoke__InnerProducer() {
    C1 controller = new C1();
    Method method = findMethod(controller, "consumerMethod");

    OutputModel outputModel1 = OutputModel.rnd();
    OutputModel outputModel2 = OutputModel.rnd();
    OutputModel outputModel3 = OutputModel.rnd();

    controller.outputModelList.add(outputModel1);
    controller.outputModelList.add(outputModel2);
    controller.outputModelList.add(outputModel3);

    InputModel inputModel1 = InputModel.rnd();
    InputModel inputModel2 = InputModel.rnd();
    InputModel inputModel3 = InputModel.rnd();

    Box box1 = new Box();
    box1.author = RND.str(10);
    box1.body = inputModel1;

    Box box2 = new Box();
    box2.author = RND.str(10);
    box2.body = inputModel2;

    Box box3 = new Box();
    box3.author = RND.str(10);
    box3.body = inputModel3;

    ConsumerRecord<byte[], Box> record1 = recordOf("test1", new byte[0], box1);
    ConsumerRecord<byte[], Box> record2 = recordOf("test1", new byte[0], box2);
    ConsumerRecord<byte[], Box> record3 = recordOf("test1", new byte[0], box3);

    ConsumerRecords<byte[], Box> records = recordsOf(asList(record1, record2, record3));

    InvokerBuilder builder = new InvokerBuilder(controller, method, null);

    Invoker invoker = builder.build();

    boolean toCommit;

    TestProducerFacade testProducer = new TestProducerFacade();

    assertThat(invoker.getUsingProducerNames()).containsExactly("test_producer");

    try (Invoker.InvokeSession invokeSession = invoker.createSession()) {

      invokeSession.putProducer("test_producer", testProducer);

      //
      //
      toCommit = invokeSession.invoke(records);
      //
      //

    }

    assertThat(toCommit).isTrue();

    assertThat(controller.inputModelList.get(0)).isEqualTo(inputModel1);
    assertThat(controller.inputModelList.get(1)).isEqualTo(inputModel2);
    assertThat(controller.inputModelList.get(2)).isEqualTo(inputModel3);
    assertThat(controller.inputModelList).hasSize(3);

    assertThat(testProducer.sentList.get(0).model).isEqualTo(outputModel1);
    assertThat(testProducer.sentList.get(1).model).isEqualTo(outputModel2);
    assertThat(testProducer.sentList.get(2).model).isEqualTo(outputModel3);
    assertThat(testProducer.sentList).hasSize(3);

    assertThat(testProducer.sentList.get(0).awaitAndGetIndex)
      .isGreaterThan(testProducer.sentList.get(2).goIndex);

    assertThat(testProducer.sentList.get(1).awaitAndGetIndex)
        .isGreaterThan(testProducer.sentList.get(2).goIndex);

    assertThat(testProducer.sentList.get(2).awaitAndGetIndex)
        .isGreaterThan(testProducer.sentList.get(2).goIndex);

    assertThat(testProducer.lastResetIndex)
      .isGreaterThan(testProducer.sentList.get(2).awaitAndGetIndex);

    assertThat(testProducer.sentList.get(0).topic).isEqualTo("outTest");
    assertThat(testProducer.sentList.get(1).topic).isEqualTo("outTest");
    assertThat(testProducer.sentList.get(2).topic).isEqualTo("outTest");

  }

  static class C2 {

    final List<InputModel> inputModelList = new ArrayList<>();
    final LinkedList<OutputModel> outputModelList = new LinkedList<>();

    @Topic({"test1"})
    @SuppressWarnings("unused")
    public void consumerMethod(InputModel inputModel,
                               @InnerProducerName("test_producer2")
                                 InnerProducerSender producer
    ) {
      inputModelList.add(inputModel);
      producer.sending(outputModelList.removeFirst()).toTopic("super_topic").go();
    }

  }

  @Test
  public void build_invoke__InnerProducerSender() {
    C2 c = new C2();
    Method method = findMethod(c, "consumerMethod");

    OutputModel outputModel1 = OutputModel.rnd();
    OutputModel outputModel2 = OutputModel.rnd();
    OutputModel outputModel3 = OutputModel.rnd();

    c.outputModelList.add(outputModel1);
    c.outputModelList.add(outputModel2);
    c.outputModelList.add(outputModel3);

    InputModel inputModel1 = InputModel.rnd();
    InputModel inputModel2 = InputModel.rnd();
    InputModel inputModel3 = InputModel.rnd();

    Box box1 = new Box();
    box1.author = RND.str(10);
    box1.body = inputModel1;

    Box box2 = new Box();
    box2.author = RND.str(10);
    box2.body = inputModel2;

    Box box3 = new Box();
    box3.author = RND.str(10);
    box3.body = inputModel3;

    ConsumerRecord<byte[], Box> record1 = recordOf("test1", new byte[0], box1);
    ConsumerRecord<byte[], Box> record2 = recordOf("test1", new byte[0], box2);
    ConsumerRecord<byte[], Box> record3 = recordOf("test1", new byte[0], box3);

    ConsumerRecords<byte[], Box> records = recordsOf(asList(record1, record2, record3));

    InvokerBuilder builder = new InvokerBuilder(c, method, null);

    Invoker invoker = builder.build();

    assertThat(invoker.getUsingProducerNames()).containsExactly("test_producer2");

    boolean toCommit;

    TestProducerFacade testProducer = new TestProducerFacade();

    try (Invoker.InvokeSession invokeSession = invoker.createSession()) {

      invokeSession.putProducer("test_producer2", testProducer);

      //
      //
      toCommit = invokeSession.invoke(records);
      //
      //

    }

    assertThat(toCommit).isTrue();

    assertThat(c.inputModelList.get(0)).isEqualTo(inputModel1);
    assertThat(c.inputModelList.get(1)).isEqualTo(inputModel2);
    assertThat(c.inputModelList.get(2)).isEqualTo(inputModel3);
    assertThat(c.inputModelList).hasSize(3);

    assertThat(testProducer.sentList.get(0).model).isEqualTo(outputModel1);
    assertThat(testProducer.sentList.get(1).model).isEqualTo(outputModel2);
    assertThat(testProducer.sentList.get(2).model).isEqualTo(outputModel3);
    assertThat(testProducer.sentList).hasSize(3);

    assertThat(testProducer.sentList.get(0).awaitAndGetIndex)
        .isGreaterThan(testProducer.sentList.get(2).goIndex);

    assertThat(testProducer.sentList.get(1).awaitAndGetIndex)
        .isGreaterThan(testProducer.sentList.get(2).goIndex);

    assertThat(testProducer.sentList.get(2).awaitAndGetIndex)
        .isGreaterThan(testProducer.sentList.get(2).goIndex);

    assertThat(testProducer.lastResetIndex)
        .isGreaterThan(testProducer.sentList.get(2).awaitAndGetIndex);

    assertThat(testProducer.sentList.get(0).topic).isEqualTo("super_topic");
    assertThat(testProducer.sentList.get(1).topic).isEqualTo("super_topic");
    assertThat(testProducer.sentList.get(2).topic).isEqualTo("super_topic");

  }

  static class C3 {

    final List<InputModel> inputModelList = new ArrayList<>();
    final LinkedList<OutputModel> outputModelList = new LinkedList<>();

    @Topic({"test1"})
    @SuppressWarnings("unused")
    public void consumerMethod(InputModel inputModel,
                               @ToTopic("outTest3")
                                 InnerProducer<OutputModel> producer
    ) {
      inputModelList.add(inputModel);
      producer.send(outputModelList.removeFirst());
    }

  }

  @Test
  public void build_invoke__InnerProducer_defaultInnerProducerName() {
    C3 controller = new C3();
    Method method = findMethod(controller, "consumerMethod");

    OutputModel outputModel1 = OutputModel.rnd();
    OutputModel outputModel2 = OutputModel.rnd();
    OutputModel outputModel3 = OutputModel.rnd();

    controller.outputModelList.add(outputModel1);
    controller.outputModelList.add(outputModel2);
    controller.outputModelList.add(outputModel3);

    InputModel inputModel1 = InputModel.rnd();
    InputModel inputModel2 = InputModel.rnd();
    InputModel inputModel3 = InputModel.rnd();

    Box box1 = new Box();
    box1.author = RND.str(10);
    box1.body = inputModel1;

    Box box2 = new Box();
    box2.author = RND.str(10);
    box2.body = inputModel2;

    Box box3 = new Box();
    box3.author = RND.str(10);
    box3.body = inputModel3;

    ConsumerRecord<byte[], Box> record1 = recordOf("test1", new byte[0], box1);
    ConsumerRecord<byte[], Box> record2 = recordOf("test1", new byte[0], box2);
    ConsumerRecord<byte[], Box> record3 = recordOf("test1", new byte[0], box3);

    ConsumerRecords<byte[], Box> records = recordsOf(asList(record1, record2, record3));

    InvokerBuilder builder = new InvokerBuilder(controller, method, null);

    Invoker invoker = builder.build();

    assertThat(invoker.getUsingProducerNames()).containsExactly(KafkaReactor.DEFAULT_INNER_PRODUCER_NAME);

    boolean toCommit;

    TestProducerFacade testProducer = new TestProducerFacade();

    try (Invoker.InvokeSession invokeSession = invoker.createSession()) {

      invokeSession.putProducer(KafkaReactor.DEFAULT_INNER_PRODUCER_NAME, testProducer);

      //
      //
      toCommit = invokeSession.invoke(records);
      //
      //

    }

    assertThat(toCommit).isTrue();

    assertThat(controller.inputModelList.get(0)).isEqualTo(inputModel1);
    assertThat(controller.inputModelList.get(1)).isEqualTo(inputModel2);
    assertThat(controller.inputModelList.get(2)).isEqualTo(inputModel3);
    assertThat(controller.inputModelList).hasSize(3);

    assertThat(testProducer.sentList.get(0).model).isEqualTo(outputModel1);
    assertThat(testProducer.sentList.get(1).model).isEqualTo(outputModel2);
    assertThat(testProducer.sentList.get(2).model).isEqualTo(outputModel3);
    assertThat(testProducer.sentList).hasSize(3);

    assertThat(testProducer.sentList.get(0).awaitAndGetIndex)
        .isGreaterThan(testProducer.sentList.get(2).goIndex);

    assertThat(testProducer.sentList.get(1).awaitAndGetIndex)
        .isGreaterThan(testProducer.sentList.get(2).goIndex);

    assertThat(testProducer.sentList.get(2).awaitAndGetIndex)
        .isGreaterThan(testProducer.sentList.get(2).goIndex);

    assertThat(testProducer.lastResetIndex)
        .isGreaterThan(testProducer.sentList.get(2).awaitAndGetIndex);

    assertThat(testProducer.sentList.get(0).topic).isEqualTo("outTest3");
    assertThat(testProducer.sentList.get(1).topic).isEqualTo("outTest3");
    assertThat(testProducer.sentList.get(2).topic).isEqualTo("outTest3");

  }

  static class C4 {

    final List<InputModel> inputModelList = new ArrayList<>();
    final LinkedList<OutputModel> outputModelList = new LinkedList<>();

    @Topic({"test1"})
    @SuppressWarnings("unused")
    @InnerProducerName("test_producer_777")
    public void consumerMethod(InputModel inputModel,
                               @ToTopic("outTest")
                                 InnerProducer<OutputModel> producer
    ) {
      inputModelList.add(inputModel);
      producer.send(outputModelList.removeFirst());
    }

  }

  @Test
  public void build_invoke__InnerProducer__innerProducerNameFromMethod() {
    C4 controller = new C4();
    Method method = findMethod(controller, "consumerMethod");

    InvokerBuilder builder = new InvokerBuilder(controller, method, null);

    Invoker invoker = builder.build();

    assertThat(invoker.getUsingProducerNames()).containsExactly("test_producer_777");
  }


  @InnerProducerName("test_producer_5423")
  public static class C5 {

    final List<InputModel> inputModelList = new ArrayList<>();
    final LinkedList<OutputModel> outputModelList = new LinkedList<>();

    @Topic({"test1", "test2"})
    @SuppressWarnings("unused")

    public void consumerMethod(InputModel inputModel,
                               @ToTopic("outTest")
                                 InnerProducer<OutputModel> producer
    ) {
      inputModelList.add(inputModel);
      producer.send(outputModelList.removeFirst());
    }

  }

  @Test
  public void build_invoke__InnerProducer__innerProducerNameFromClass() {
    C5 controller = new C5();
    Method method = findMethod(controller, "consumerMethod");

    InvokerBuilder builder = new InvokerBuilder(controller, method, null);

    Invoker invoker = builder.build();

    assertThat(invoker.getUsingProducerNames()).containsExactly("test_producer_5423");
  }

  @InnerProducerName("test_producer_5423")
  public static class C6 {

    final List<InputModel> inputModelList = new ArrayList<>();
    final LinkedList<OutputModel> outputModelList = new LinkedList<>();

    @Topic({"test1", "test2"})
    @SuppressWarnings("unused")
    public void consumerMethod(InputModel inputModel,
                                   InnerProducer<OutputModel> producer
    ) {
      inputModelList.add(inputModel);
      producer.send(outputModelList.removeFirst());
    }

  }

  @Test(expectedExceptions = AbsentAnnotationToTopicOverInnerProducer.class)
  public void build_invoke__InnerProducer__withoutToTopic() {
    C6 controller = new C6();
    Method method = findMethod(controller, "consumerMethod");

    InvokerBuilder builder = new InvokerBuilder(controller, method, null);

    builder.build();
  }


}
