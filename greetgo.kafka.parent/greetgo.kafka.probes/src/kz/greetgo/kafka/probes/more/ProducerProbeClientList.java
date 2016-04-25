package kz.greetgo.kafka.probes.more;

import kz.greetgo.kafka.core.StrConverterPreparationBased;
import kz.greetgo.kafka.producer.AbstractKafkaSender;
import kz.greetgo.kafka.producer.KafkaSending;
import kz.greetgo.kafka.str.StrConverter;
import kz.greetgo.kafka.str.StrConverterXml;

import java.util.ArrayList;
import java.util.List;

public class ProducerProbeClientList {

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
    protected String topic() {
      return Params.TOPIC_NAME;
    }
  }

  public static void main(String[] args) throws Exception {
    MyKafkaSender so = new MyKafkaSender();
    try (KafkaSending kafkaSending = so.open()) {

      for (int i = 20; i < 30; i++) {
        List<Object> list = new ArrayList<>();
        String id = "client-" + (i + 101_000_000);

        list.add(new ChangeClientSurname(id, "Иванов " + id));
        list.add(new ChangeClientName(id, "Иван " + id));

        kafkaSending.send(list);
      }

    }
    System.out.println("OK");
  }

}
