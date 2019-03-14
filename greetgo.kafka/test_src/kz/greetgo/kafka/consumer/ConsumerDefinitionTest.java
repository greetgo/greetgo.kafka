package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.consumer.annotations.ConsumerName;
import kz.greetgo.kafka.consumer.annotations.ConsumersFolder;
import kz.greetgo.kafka.consumer.annotations.GroupId;
import kz.greetgo.kafka.consumer.annotations.KafkaNotifier;
import kz.greetgo.kafka.consumer.annotations.Topic;
import kz.greetgo.kafka.model.Box;
import kz.greetgo.util.RND;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.List;

import static kz.greetgo.kafka.util_for_tests.ReflectionUtil.findMethod;
import static org.fest.assertions.api.Assertions.assertThat;

public class ConsumerDefinitionTest {

  static class C_getAutoOffsetReset {
    @Topic("test1")
    @GroupId("CoolGroupId1")
    @SuppressWarnings("unused")
    public void method1(Box box) {}

    @KafkaNotifier
    @Topic("test1")
    @GroupId("CoolGroupId2")
    @SuppressWarnings("unused")
    public void method2(Box box) {}
  }

  @Test
  public void getAutoOffsetReset_EARLIEST() {
    C_getAutoOffsetReset controller = new C_getAutoOffsetReset();
    Method method = findMethod(controller, "method1");

    ConsumerDefinition consumerDefinition = new ConsumerDefinition(controller, method, null, null);

    //
    //
    AutoOffsetReset autoOffsetReset = consumerDefinition.getAutoOffsetReset();
    //
    //

    assertThat(autoOffsetReset).isEqualTo(AutoOffsetReset.EARLIEST);
  }

  @Test
  public void getAutoOffsetReset_LATEST() {
    C_getAutoOffsetReset controller = new C_getAutoOffsetReset();
    Method method = findMethod(controller, "method2");

    String hostId = RND.str(10);

    ConsumerDefinition consumerDefinition = new ConsumerDefinition(controller, method, null, hostId);

    //
    //
    AutoOffsetReset autoOffsetReset = consumerDefinition.getAutoOffsetReset();
    String groupId = consumerDefinition.getGroupId();
    //
    //

    assertThat(autoOffsetReset).isEqualTo(AutoOffsetReset.LATEST);
    assertThat(groupId).isEqualTo("CoolGroupId2" + hostId);
  }

  static class C_getGroupId {
    @Topic("test1")
    @SuppressWarnings("unused")
    public void methodHelloWorld(Box box) {}

    @Topic("test1")
    @SuppressWarnings("unused")
    @GroupId("Hello_World_Group_ID")
    public void methodForAnnotation() {}
  }

  @Test
  public void getGroupId_fromMethod() {
    C_getGroupId controller = new C_getGroupId();
    Method method = findMethod(controller, "methodHelloWorld");

    ConsumerDefinition consumerDefinition = new ConsumerDefinition(controller, method, null, null);

    //
    //
    String groupId = consumerDefinition.getGroupId();
    //
    //

    assertThat(groupId).isEqualTo("methodHelloWorld");
  }

  @Test
  public void getGroupId_fromAnnotation() {
    C_getGroupId controller = new C_getGroupId();
    Method method = findMethod(controller, "methodForAnnotation");

    ConsumerDefinition consumerDefinition = new ConsumerDefinition(controller, method, null, null);

    //
    //
    String groupId = consumerDefinition.getGroupId();
    //
    //

    assertThat(groupId).isEqualTo("Hello_World_Group_ID");
  }

  static class C_getFolderPath_null {

    @Topic("topic")
    @SuppressWarnings("unused")
    public void method1() {}

  }

  @Test
  public void getFolderPath_null() {
    C_getFolderPath_null controller = new C_getFolderPath_null();
    Method method = findMethod(controller, "method1");

    ConsumerDefinition consumerDefinition = new ConsumerDefinition(controller, method, null, null);

    //
    //
    String getFolderPath = consumerDefinition.getFolderPath();
    //
    //

    assertThat(getFolderPath).isNull();

  }

  @ConsumersFolder("hi_from_consumers_path")
  static class C_getFolderPath_ok {

    @Topic("topic")
    @SuppressWarnings("unused")
    public void method1() {}

  }

  @Test
  public void getFolderPath_ok() {
    C_getFolderPath_ok controller = new C_getFolderPath_ok();
    Method method = findMethod(controller, "method1");

    ConsumerDefinition consumerDefinition = new ConsumerDefinition(controller, method, null, null);

    //
    //
    String getFolderPath = consumerDefinition.getFolderPath();
    //
    //

    assertThat(getFolderPath).isEqualTo("hi_from_consumers_path");

  }


  static class C_getConsumerName {

    @Topic("topic")
    @SuppressWarnings("unused")
    public void helloWorldConsumerWOW() {}

    @Topic("topic")
    @ConsumerName("ThisIsACoolConsumer")
    @SuppressWarnings("unused")
    public void method2() {}
  }

  @Test
  public void getConsumerName_fromMethod() {
    C_getConsumerName controller = new C_getConsumerName();
    Method method = findMethod(controller, "helloWorldConsumerWOW");

    ConsumerDefinition consumerDefinition = new ConsumerDefinition(controller, method, null, null);

    //
    //
    String consumerName = consumerDefinition.getConsumerName();
    //
    //

    assertThat(consumerName).isEqualTo("helloWorldConsumerWOW");
  }

  @Test
  public void getConsumerName_fromAnnotation() {
    C_getConsumerName controller = new C_getConsumerName();
    Method method = findMethod(controller, "method2");

    ConsumerDefinition consumerDefinition = new ConsumerDefinition(controller, method, null, null);

    //
    //
    String consumerName = consumerDefinition.getConsumerName();
    //
    //

    assertThat(consumerName).isEqualTo("ThisIsACoolConsumer");
  }

  static class C_isAutoCommit {

    @Topic("topic")
    @SuppressWarnings("unused")
    public void method1() {}

  }

  @Test
  public void isAutoCommit() {
    C_isAutoCommit controller = new C_isAutoCommit();
    Method method = findMethod(controller, "method1");

    ConsumerDefinition consumerDefinition = new ConsumerDefinition(controller, method, null, null);

    //
    //
    boolean autoCommit = consumerDefinition.isAutoCommit();
    //
    //

    assertThat(autoCommit).isFalse();
  }

  @ConsumersFolder("topFolder")
  static class C_logDisplay {

    @Topic("topic")
    @SuppressWarnings("unused")
    @ConsumerName("ThisIsACoolConsumer")
    public void method1() {}

  }

  @Test
  public void logDisplay_fromAnnotation() {
    C_logDisplay controller = new C_logDisplay();
    Method method = findMethod(controller, "method1");

    ConsumerDefinition consumerDefinition = new ConsumerDefinition(controller, method, null, null);

    //
    //
    String logDisplay = consumerDefinition.logDisplay();
    //
    //

    assertThat(logDisplay).isEqualTo("topFolder/C_logDisplay.method1[ThisIsACoolConsumer]");
  }

  static class C2_logDisplay {

    @Topic("topic")
    @SuppressWarnings("unused")
    public void coolMethodName() {}

  }

  @Test
  public void logDisplay_fromMethod() {
    C2_logDisplay controller = new C2_logDisplay();
    Method method = findMethod(controller, "coolMethodName");

    ConsumerDefinition consumerDefinition = new ConsumerDefinition(controller, method, null, null);

    //
    //
    String logDisplay = consumerDefinition.logDisplay();
    //
    //

    assertThat(logDisplay).isEqualTo("C2_logDisplay.[coolMethodName]");
  }

  static class ClassFor_topicList {

    @Topic("topicHelloWorld")
    @SuppressWarnings("unused")
    public void method1() {}

    @Topic({"topicA1", "topicB2"})
    @SuppressWarnings("unused")
    public void method2() {}

  }

  @Test
  public void topicList_oneTopic() {
    ClassFor_topicList controller = new ClassFor_topicList();
    Method method = findMethod(controller, "method1");

    ConsumerDefinition consumerDefinition = new ConsumerDefinition(controller, method, null, null);

    //
    //
    List<String> topicList = consumerDefinition.topicList();
    //
    //

    assertThat(topicList).hasSize(1);
    assertThat(topicList.get(0)).isEqualTo("topicHelloWorld");
  }

  @Test
  public void topicList_twoTopics() {
    ClassFor_topicList controller = new ClassFor_topicList();
    Method method = findMethod(controller, "method2");

    ConsumerDefinition consumerDefinition = new ConsumerDefinition(controller, method, null, null);

    //
    //
    List<String> topicList = consumerDefinition.topicList();
    //
    //

    assertThat(topicList).hasSize(2);
    assertThat(topicList.get(0)).isEqualTo("topicA1");
    assertThat(topicList.get(1)).isEqualTo("topicB2");
  }
}
