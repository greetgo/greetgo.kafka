package kz.greetgo.kafka2.core;

import kz.greetgo.kafka2.ModelKryo;
import kz.greetgo.kafka2.ModelKryo2;
import kz.greetgo.kafka2.consumer.TestConsumerLogger;
import kz.greetgo.kafka2.consumer.annotations.ConsumersFolder;
import kz.greetgo.kafka2.consumer.annotations.GroupId;
import kz.greetgo.kafka2.consumer.annotations.Topic;
import kz.greetgo.kafka2.core.config.ConfigStorageInMem;
import kz.greetgo.kafka2.util.NetUtil;
import org.testng.SkipException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class KafkaReactorImplTest {

  String bootstrapServers;

  @BeforeMethod
  public void pingKafka() {
    bootstrapServers = "localhost:9092";

    if (!NetUtil.canConnectToAnyBootstrapServer(bootstrapServers)) {
      throw new SkipException("No kafka connection : " + bootstrapServers);
    }
  }

  @ConsumersFolder("top")
  public static class TestController {

    @Topic("test_topic1")
    @GroupId("test_topic_wow")
    public void consumer1(ModelKryo model) {
      System.out.println("Come from consumer1: " + model);
    }

    @Topic("test_topic2")
    @GroupId("test_topic_wow")
    public void consumer2(ModelKryo2 model) {
      System.out.println("Come from consumer2: " + model);
    }

  }


  @Test
  public void runKafkaReactor() throws Exception {
    TestController controller = new TestController();

    TestConsumerLogger testConsumerLogger = new TestConsumerLogger();

    ConfigStorageInMem configStorage = new ConfigStorageInMem();

    KafkaReactor kafkaReactor = new KafkaReactorImpl();
    kafkaReactor.setConfigStorage(configStorage);

    kafkaReactor.addController(controller);

    kafkaReactor.registerModel(ModelKryo.class);
    kafkaReactor.registerModel(ModelKryo2.class);

    kafkaReactor.setConsumerLogger(testConsumerLogger);

    kafkaReactor.setHostId("testHost");
    kafkaReactor.setAuthorGetter(() -> "author123");

    kafkaReactor.setBootstrapServers(() -> bootstrapServers);

    kafkaReactor.startConsumers();

    File runFile = new File("build/runKafkaReactor.txt");

    runFile.getParentFile().mkdirs();
    runFile.createNewFile();

    while (runFile.exists()) {
      Thread.sleep(600);
    }
  }
}
