package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.core.*;
import kz.greetgo.kafka.producer.AbstractKafkaSender;
import kz.greetgo.kafka.producer.KafkaSending;
import kz.greetgo.kafka.str.StrConverter;
import kz.greetgo.kafka.str.StrConverterXml;
import kz.greetgo.util.RND;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Arrays.asList;
import static kz.greetgo.util.ServerUtil.notNull;
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
    Caller caller = UtilCaller.createCaller(testing, method);
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
  public void createCaller_listObject_2() throws Exception {

    Method method = Testing.class.getMethod("listObject", List.class);

    Testing testing = new Testing();

    //
    //
    Caller caller = UtilCaller.createCaller(testing, method);
    //
    //

    assertThat(caller).isNotNull();

    List<Box> list = new ArrayList<>();
    list.add(boxWithBody("a"));
    list.add(boxWithBody(asList("b", "c")));
    list.add(boxWithBody(asList(asList("d", "e"), "f")));

    caller.call(list);

    assertThat(testing.listObject_1).containsExactly("a", "b", "c", "d", "e", "f");
  }

  private Box boxWithBody(Object body) {
    Box box = new Box();
    box.head = new Head();
    box.head.a = RND.str(10);
    box.body = body;
    return box;
  }

  @Test
  public void createCaller_listObject() throws Exception {

    Method method = Testing.class.getMethod("listObject", List.class);

    Testing testing = new Testing();

    //
    //
    Caller caller = UtilCaller.createCaller(testing, method);
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
    Caller caller = UtilCaller.createCaller(testing, method);
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
    Caller caller = UtilCaller.createCaller(testing, method);
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
    Caller caller = UtilCaller.createCaller(testing, method);
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

  public static class MyTopicManager extends AbstractKafkaTopicManager {

    private KafkaParams kafkaParams;

    public MyTopicManager(KafkaParams kafkaParams) {
      this.kafkaParams = kafkaParams;
    }

    @Override
    protected String zookeeperServers() {
      return kafkaParams.zookeeperServers();
    }
  }


  public static class Client implements HasId, Comparable<Client> {
    public String id;
    public String surname;
    public String name;

    public Client() {
    }

    public Client(String id, String surname, String name) {
      this.id = id;
      this.surname = surname;
      this.name = name;
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public String toString() {
      return "Client{" +
          "id='" + id + '\'' +
          ", surname='" + surname + '\'' +
          ", name='" + name + '\'' +
          '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Client client = (Client) o;

      if (id != null ? !id.equals(client.id) : client.id != null) return false;
      if (surname != null ? !surname.equals(client.surname) : client.surname != null) return false;
      return name != null ? name.equals(client.name) : client.name == null;

    }

    @Override
    public int hashCode() {
      int result = id != null ? id.hashCode() : 0;
      result = 31 * result + (surname != null ? surname.hashCode() : 0);
      result = 31 * result + (name != null ? name.hashCode() : 0);
      return result;
    }

    @Override
    public int compareTo(Client o) {
      return notNull(id).compareTo(notNull(o.id));
    }
  }

  private static void prepareStrConverter(StrConverter strConverter) {
    StrConverterPreparationBased.prepare(strConverter);
    strConverter.useClass(Client.class, "Client");
  }

  static class MySender extends AbstractKafkaSender {

    private KafkaParams kafkaParams;

    public MySender(KafkaParams kafkaParams) {
      this.kafkaParams = kafkaParams;
    }

    @Override
    protected String getBootstrapServers() {
      return kafkaParams.kafkaServers();
    }

    @Override
    protected StrConverter createStrConverter() {
      StrConverter ret = new StrConverterXml();
      prepareStrConverter(ret);
      return ret;
    }

    public String author = "left author";

    @Override
    protected String author() {
      return author;
    }

    @Override
    protected Set<String> ignorableConsumers(String author, Object sendingObject, String key) {
      return null;
    }

    @Override
    protected String topic() {
      return TEST_TOPIC_NAME;
    }
  }

  static class TestConsumerManager extends AbstractNewConsumerManager {
    private KafkaParams kafkaParams;

    public TestConsumerManager(KafkaParams kafkaParams) {
      this.kafkaParams = kafkaParams;
    }

    @Override
    protected String bootstrapServers() {
      return kafkaParams.kafkaServers();
    }

    @Override
    protected String cursorIdPrefix() {
      return "";
    }

    @Override
    protected String topicPrefix() {
      return "";
    }

    @Override
    protected String soulId() {
      return "asd";
    }

    @Override
    protected StrConverter strConverter() {
      return kafkaParams.createStrConverter();
    }

    @Override
    protected void handleCallException(ConsumerDefinition consumerDefinition, Exception exception) {
      throw new RuntimeException(exception);
    }
  }

  static class TestConsumers {
    public final Map<String, Client> clientMap = new ConcurrentHashMap<>();
    int i = 0;

    @Consume(name = "testConsumerName", cursorId = TEST_TOPIC_NAME + "-main-cursor-3", topics = TEST_TOPIC_NAME)
    public void someClients(Client client) {
      clientMap.put(client.id, client);
      System.out.println(++i + " Consumer gets " + client);
    }
  }

  public static final String TEST_TOPIC_NAME = "AbstractConsumerManagerTest_001";

  interface KafkaParams {
    String zookeeperServers();

    String kafkaServers();

    StrConverter createStrConverter();
  }

  static class KafkaParamsImpl implements KafkaParams {

    @Override
    public String zookeeperServers() {
      return "localhost:2181";
    }

    @Override
    public String kafkaServers() {
      return "localhost:9092";
    }

    @Override
    public StrConverter createStrConverter() {
      StrConverter ret = new StrConverterXml();
      prepareStrConverter(ret);
      return ret;
    }
  }

  @Test(timeOut = 30_000)
  public void consumer() throws Exception {
    KafkaParams kafkaParams = new KafkaParamsImpl();

    MySender sender = new MySender(kafkaParams);

    int clientCount = 3;

    final Map<String, Client> clientMap = new HashMap<>();

    try (KafkaSending sending = sender.open()) {
      for (int u = 0; u < clientCount; u++) {
        Client c = new Client();
        c.id = u + "-" + RND.intStr(10);
        c.surname = "surname " + RND.plusInt(100000);
        c.name = "name " + RND.plusInt(100000);

        System.out.println("Sending " + c);
        sending.send(c);
        clientMap.put(c.id, c);
      }
    }

    //if ("a".equals("a")) return;

    TestConsumerManager consumerManager = new TestConsumerManager(kafkaParams);

    TestConsumers testConsumers = new TestConsumers();
    consumerManager.registerBean(testConsumers);

    String consumeName = "testConsumerName";

    consumerManager.setWorkingThreads(consumeName, 3);

    Thread t[] = new Thread[Thread.activeCount()];
    Thread.enumerate(t);

    int consumeThreadCount = 0;
    for (Thread thread : t) {
      if (thread.getName().startsWith(consumeName)) consumeThreadCount++;
      System.out.println(thread.getName());
    }

    assertThat(consumeThreadCount).isEqualTo(3);


    while (!testConsumers.clientMap.keySet().containsAll(clientMap.keySet())) {
      Thread.sleep(100);
    }

    consumerManager.stopAll();

    Set<String> ids = new HashSet<>();
    ids.addAll(testConsumers.clientMap.keySet());
    for (String id : ids) {
      if (!clientMap.keySet().contains(id)) {
        testConsumers.clientMap.remove(id);
      }
    }

    assertThat(testConsumers.clientMap).isEqualTo(clientMap);
  }

  @Test(timeOut = 30_000, enabled = false)
  public void removeTopicClient() throws Exception {
    KafkaParams kafkaParams = new KafkaParamsImpl();
    MyTopicManager topicManager = new MyTopicManager(kafkaParams);
    topicManager.removeTopic(TEST_TOPIC_NAME);
  }
}