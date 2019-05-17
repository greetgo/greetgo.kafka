package kz.greetgo.kafka_old.probes.more;

import kz.greetgo.kafka_old.consumer.AbstractConsumerManager;
import kz.greetgo.kafka_old.consumer.Consume;
import kz.greetgo.kafka_old.consumer.ConsumerDefinition;
import kz.greetgo.kafka_old.core.StrConverterPreparationBased;
import kz.greetgo.kafka_old.str.StrConverterXml;
import kz.greetgo.strconverter.StrConverter;

import java.util.ArrayList;
import java.util.List;

public class ConsumerProbeClient {

  private static StrConverter createStrConverter() {
    StrConverter ret = new StrConverterXml();
    StrConverterPreparationBased.prepare(ret);
    ret.useClass(Client.class, "Client");
    return ret;
  }

  public static class ProbeConsumers {

    public final List<Object> clientList = new ArrayList<>();

    @Consume(name = "test", cursorId = "cursor-B", topics = Params.TOPIC_NAME)
    public void someClients(List<Object> clientList) {
      this.clientList.addAll(clientList);
      System.out.println(clientList);
    }

  }

  public static class ProbeConsumerManager extends AbstractConsumerManager {
    @Override
    protected String bootstrapServers() {
      return "localhost:9092";
    }

    @Override
    protected String zookeeperConnectStr() {
      return "localhost:2181";
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
      return createStrConverter();
    }

    @Override
    protected void handleCallException(ConsumerDefinition consumerDefinition, Exception exception) {
      throw new RuntimeException(exception);
    }
  }

  public static void main(String[] args) throws Exception {
    ProbeConsumerManager consumerManager = new ProbeConsumerManager();

    ProbeConsumers probeConsumers = new ProbeConsumers();

    consumerManager.registerBean(probeConsumers);

    for (String consumerName : consumerManager.consumerNames()) {
      consumerManager.setWorkingThreads(consumerName, 1);
    }

    consumerManager.allConsumerNamesReadingTopic("asd1")
      .stream()
      .map(consumerManager::getCursorIdByConsumerName)
      .forEachOrdered(cursorId -> {
        System.out.println("cursorId = " + cursorId);
        System.out.println("cursorId = " + cursorId);
      });
    consumerManager.allConsumerNamesReadingTopic("asd2")
      .stream()
      .map(consumerManager::getCursorIdByConsumerName)
      .forEachOrdered(cursorId -> {
        System.out.println("cursorId = " + cursorId);
        System.out.println("cursorId = " + cursorId);
      });
  }
}