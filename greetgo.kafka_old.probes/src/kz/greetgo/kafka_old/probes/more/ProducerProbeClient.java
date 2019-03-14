package kz.greetgo.kafka_old.probes.more;

import kz.greetgo.kafka_old.core.StrConverterPreparationBased;
import kz.greetgo.kafka_old.producer.AbstractKafkaSender;
import kz.greetgo.kafka_old.producer.KafkaSending;
import kz.greetgo.kafka_old.str.StrConverterXml;
import kz.greetgo.strconverter.StrConverter;

import java.util.Set;

public class ProducerProbeClient {

  static class MyKafkaSender extends AbstractKafkaSender {

    @Override
    protected String getBootstrapServers() {
      return "localhost:9092";
    }

    @Override
    protected StrConverter createStrConverter() {
      StrConverterXml ret = new StrConverterXml();
      StrConverterPreparationBased.prepare(ret);
      ret.useClass(Client.class, Client.class.getSimpleName());
      return ret;
    }

    @Override
    protected String author() {
      return "asd";
    }

    @Override
    protected Set<String> ignorableConsumers(String author, Object sendingObject, String key) {
      return null;
    }

    @Override
    protected String topic() {
      return Params.TOPIC_NAME;
    }
  }

  public static void main(String[] args) throws Exception {
    MyKafkaSender so = new MyKafkaSender();
    try (KafkaSending kafkaSending = so.open()) {
      for (int i = 10; i < 20; i++) {
        String I = "" + (i + 101_000_000);
        Client client = new Client();
        client.id = "client-" + I;
        client.surname = "Иванов " + I;
        client.name = "Иван " + I;
        kafkaSending.send(client);
      }
    }
    System.out.println("OK");
  }

}
