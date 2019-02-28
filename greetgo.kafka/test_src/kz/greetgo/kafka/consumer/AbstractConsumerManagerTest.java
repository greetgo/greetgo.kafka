package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.core.AbstractKafkaTopicManager;
import kz.greetgo.kafka.core.HasId;
import kz.greetgo.kafka.core.StrConverterPreparationBased;
import kz.greetgo.kafka.producer.AbstractKafkaSender;
import kz.greetgo.kafka.producer.KafkaSending;
import kz.greetgo.kafka.str.StrConverterXml;
import kz.greetgo.strconverter.StrConverter;
import kz.greetgo.util.RND;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static kz.greetgo.util.ServerUtil.notNull;
import static org.fest.assertions.api.Assertions.assertThat;

public class AbstractConsumerManagerTest {


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

  static class TestConsumerManager extends AbstractConsumerManager {
    private KafkaParams kafkaParams;

    public TestConsumerManager(KafkaParams kafkaParams) {
      this.kafkaParams = kafkaParams;
    }

    @Override
    protected String bootstrapServers() {
      return kafkaParams.kafkaServers();
    }

    @Override
    protected String zookeeperConnectStr() {
      return kafkaParams.zookeeperServers();
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
