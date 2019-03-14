package kz.greetgo.kafka.consumer;

import com.esotericsoftware.kryo.Kryo;
import kz.greetgo.kafka.ModelKryo;
import kz.greetgo.kafka.consumer.annotations.ConsumersFolder;
import kz.greetgo.kafka.consumer.annotations.GroupId;
import kz.greetgo.kafka.consumer.annotations.Topic;
import kz.greetgo.kafka.core.config.EventConfigStorageInMem;
import kz.greetgo.kafka.model.Box;
import kz.greetgo.kafka.util.NetUtil;
import org.testng.SkipException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static org.fest.assertions.api.Assertions.assertThat;

public class ConsumerReactorTest {

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

    @SuppressWarnings("unused")
    @Topic("test_topic")
    @GroupId("test_topic_wow")
    public void consumer(ModelKryo model) {
      System.out.println("Come from kafka: " + model);
    }

  }

  @Test
  public void startStop() {

    TestController controller = new TestController();

    TestConsumerLogger testConsumerLogger = new TestConsumerLogger();

    ConsumerDefinitionExtractor cde = new ConsumerDefinitionExtractor();
    cde.consumerLogger = testConsumerLogger;
    cde.hostId = "testHost";

    List<ConsumerDefinition> consumerDefinitionList = cde.extract(controller);

    assertThat(consumerDefinitionList).hasSize(1);

    Kryo kryo = new Kryo();
    kryo.register(Box.class);
    kryo.register(ModelKryo.class);

    EventConfigStorageInMem configStorage = new EventConfigStorageInMem();

    ConsumerReactor consumerReactor = new ConsumerReactor();
    consumerReactor.kryo = kryo;
    consumerReactor.configStorage = configStorage;
    consumerReactor.bootstrapServers = () -> bootstrapServers;
    consumerReactor.consumerLogger = testConsumerLogger;
    consumerReactor.consumerDefinition = consumerDefinitionList.get(0);
    consumerReactor.storageRootPath = "test/root";
    consumerReactor.storageParentConfigPath = "test/root/parent.txt";

    consumerReactor.start();

    System.out.println();
    configStorage.printCurrentState();

    consumerReactor.stop();
  }
}
