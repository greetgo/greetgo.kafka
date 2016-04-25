package kz.greetgo.kafka.probes.more;

import kz.greetgo.kafka.consumer.AbstractConsumerManager;
import kz.greetgo.kafka.consumer.Consume;
import kz.greetgo.kafka.core.StrConverterPreparationBased;
import kz.greetgo.kafka.str.StrConverter;
import kz.greetgo.kafka.str.StrConverterXml;

import java.lang.reflect.Method;
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

    @Consume(groupId = "cursor-B", topics = Params.TOPIC_NAME)
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
    protected StrConverter strConverter() {
      return createStrConverter();
    }

    @Override
    protected void handleCallException(Object bean, Method method, Exception exception) {
      throw new RuntimeException(exception);
    }
  }

  public static void main(String[] args) throws Exception {
    ProbeConsumerManager consumerManager = new ProbeConsumerManager();

    ProbeConsumers probeConsumers = new ProbeConsumers();

    consumerManager.appendBean(probeConsumers);

    consumerManager.startup();
  }
}
