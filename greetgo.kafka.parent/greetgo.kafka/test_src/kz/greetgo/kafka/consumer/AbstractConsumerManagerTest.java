package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.Servers;
import kz.greetgo.kafka.consumer.AbstractConsumerManager.Caller;
import kz.greetgo.kafka.core.Box;
import kz.greetgo.kafka.core.HasId;
import kz.greetgo.kafka.core.Head;
import kz.greetgo.kafka.core.StrConverterPreparation;
import kz.greetgo.kafka.producer.AbstractKafkaSender;
import kz.greetgo.kafka.producer.KafkaSending;
import kz.greetgo.kafka.str.StrConverter;
import kz.greetgo.kafka.str.StrConverterXml;
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
    StrConverterPreparation.prepare(strConverter);
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

    public String author = "asd";

    @Override
    protected String author() {
      return author;
    }

    @Override
    protected String topic() {
      return "client";
    }
  }

  static class TestConsumerManager extends AbstractConsumerManager {
    public int port;

    @Override
    protected String bootstrapServers() {
      return "localhost:" + port;
    }

    @Override
    protected void handleCallException(Object bean, Method method, Exception exception) {
      throw new RuntimeException(exception);
    }
  }

  static class TestConsumers {

    public final List<Client> clientList = new ArrayList<>();

    @Consume(groupId = "asd", topics = "client")
    public void someClients(Client client) {
      clientList.add(client);
      System.out.println(client);
    }

  }

  @Test
  public void startup_shutdown() throws Exception {
    Servers servers = new Servers();
    servers.tmpDir = "build/asd";

    servers.startupAll();

    MySender senderOpener = new MySender();
    senderOpener.port = servers.kafkaServerPort;

    try (KafkaSending ks = senderOpener.open()) {

      ks.send(new Client("client-001", "Иванов", "Иван"));
      ks.send(new Client("client-002", "Петров", "Пётр"));

    }

    TestConsumerManager consumerManager = new TestConsumerManager();
    consumerManager.strConverter = senderOpener.strConverter();

    TestConsumers testConsumers = new TestConsumers();

    consumerManager.appendBean(testConsumers);

    consumerManager.startup();

    System.out.println("Sleep");
    Thread.sleep(5000);

    System.out.println("Check point 1");

    consumerManager.shutdownAndJoin();

    System.out.println("Check point 2");

    servers.shutdownAll();

    System.out.println("Shut downed");
    //servers.clean();

  }
}