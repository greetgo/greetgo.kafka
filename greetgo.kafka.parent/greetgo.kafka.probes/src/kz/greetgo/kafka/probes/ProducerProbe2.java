package kz.greetgo.kafka.probes;

import kz.greetgo.kafka.core.HasId;
import kz.greetgo.kafka.core.StrConverterPreparationBased;
import kz.greetgo.kafka.producer.AbstractKafkaSender;
import kz.greetgo.kafka.producer.KafkaSending;
import kz.greetgo.kafka.str.StrConverter;
import kz.greetgo.kafka.str.StrConverterXml;

import java.util.Set;

public class ProducerProbe2 {

  public static class Client implements HasId {
    public String id;
    public String surname, name;

    @Override
    public String getId() {
      return id;
    }
  }

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
    protected Set<String> ignorableConsumers(String author, Object sendingObject, String key, String value) {
      return null;
    }

    @Override
    protected String topic() {
      return "client";
    }
  }


  public static void main(String[] args) throws Exception {
    MyKafkaSender so = new MyKafkaSender();

    try (KafkaSending kafkaSending = so.open()) {

      for (int i = 10; i < 20; i++) {
        String I = "" + i;
        while (I.length() < 5) I = "0" + I;
        Client client = new Client();
        client.id = "asd-" + I;
        client.surname = "Иванов " + I;
        client.name = "Иван" + I;

        kafkaSending.send(client);
      }
    }

    System.out.println("OK");
  }

}
