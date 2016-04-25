package kz.greetgo.kafka.consumer;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kz.greetgo.kafka.Servers;
import kz.greetgo.kafka.consumer.AbstractConsumerManager.Caller;
import kz.greetgo.kafka.core.Box;
import kz.greetgo.kafka.core.HasId;
import kz.greetgo.kafka.core.Head;
import kz.greetgo.kafka.core.StrConverterPreparationBased;
import kz.greetgo.kafka.producer.AbstractKafkaSender;
import kz.greetgo.kafka.producer.KafkaSending;
import kz.greetgo.kafka.str.StrConverter;
import kz.greetgo.kafka.str.StrConverterXml;
import kz.greetgo.util.RND;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static java.util.Arrays.asList;
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
  public void createCaller_listObject_2() throws Exception {

    Method method = Testing.class.getMethod("listObject", List.class);

    Testing testing = new Testing();

    //
    //
    Caller caller = AbstractConsumerManager.createCaller(testing, method);
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

  static class Client implements HasId {
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
  }

  private static void prepareStrConverter(StrConverter strConverter) {
    StrConverterPreparationBased.prepare(strConverter);
    strConverter.useClass(Client.class, "Client");
  }

  static class MySender extends AbstractKafkaSender {

    public int port;

    @Override
    protected String getBootstrapServers() {
      return "localhost:" + port;
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
    protected String topic() {
      return TEST_TOPIC_NAME;
    }
  }

  static class TestConsumerManager extends AbstractConsumerManager {
    public int port;

    @Override
    protected String bootstrapServers() {
      return "localhost:" + port;
    }

    StrConverter strConverter = null;

    @Override
    protected StrConverter strConverter() {
      return strConverter;
    }

    @Override
    protected void handleCallException(Object bean, Method method, Exception exception) {
      throw new RuntimeException(exception);
    }
  }

  static class TestConsumers {

    public final List<Client> clientList = new ArrayList<>();

    @Consume(groupId = "asd", topics = TEST_TOPIC_NAME)
    public void someClients(Client client) {
      clientList.add(client);
      System.out.println(client);
    }

  }

  public static final String TEST_TOPIC_NAME = "client";

  @Test(timeOut = 30_000, enabled = false)
  public void startup_shutdown() throws Exception {
    Servers servers = new Servers();
    servers.tmpDir = "build/startup_shutdown_" + RND.str(10);

    servers.startupAll();

    System.out.println("---- point 001");

    {
      ZkConnection zkConnection = new ZkConnection("localhost:" + servers.zookeeperClientPort, 3000);
      try {
        ZkClient zkClient = new ZkClient(zkConnection, 3000, ZKStringSerializer$.MODULE$);
        ZkUtils zkUtils = new ZkUtils(zkClient, zkConnection, false);
        int partitions = 1, replicationFactor = 1;
        AdminUtils.createTopic(zkUtils, TEST_TOPIC_NAME, partitions, replicationFactor, new Properties());
      } finally {
        zkConnection.close();
      }
    }

    System.out.println("---- point 002");

    MySender senderOpener = new MySender();
    senderOpener.port = servers.kafkaServerPort;

    try (KafkaSending ks = senderOpener.open()) {

      ks.send(new Client("client-001", "Иванов", "Иван"));
      ks.send(new Client("client-002", "Петров", "Пётр"));

    }

    System.out.println("---- point 003");

    TestConsumerManager consumerManager = new TestConsumerManager();
    consumerManager.strConverter = senderOpener.strConverter();
    consumerManager.port = servers.kafkaServerPort;

    TestConsumers testConsumers = new TestConsumers();

    consumerManager.appendBean(testConsumers);

    consumerManager.startup();

    while (testConsumers.clientList.size() < 2) {
      System.out.println("--- -- ---- -- ---- ---- -- -- --- - ---- - ----" +
          " Sleep testConsumers.clientList.size() = " + testConsumers.clientList.size());
      Thread.sleep(300);
    }

    System.out.println("Check point 1");

    consumerManager.shutdownAndJoin();

    System.out.println("Check point 2");

    servers.shutdownAll();

    System.out.println("Shut downed");
    //servers.clean();

  }
}