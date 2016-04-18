package kz.greetgo.kafka.probes;

import kz.greetgo.kafka.core.HasId;
import kz.greetgo.kafka.producer.AbstractKafkaSenderOpener;
import kz.greetgo.kafka.producer.KafkaSender;
import kz.greetgo.kafka.str.StrConverter;

public class ProducerProbe2 {

  public static class Client implements HasId {
    public String id;
    public String surname, name;

    @Override
    public String getId() {
      return id;
    }
  }

  static class MyKafkaSenderOpener extends AbstractKafkaSenderOpener {

    @Override
    protected String getBootstrapServers() {
      return "localhost:9092";
    }

    @Override
    protected void prepareStrConverter(StrConverter strConverter) {
      strConverter.useClass(Client.class, Client.class.getSimpleName());
    }

    @Override
    protected String author() {
      return "asd";
    }

    @Override
    protected String topic() {
      return "client";
    }
  }


  public static void main(String[] args) throws Exception {
    MyKafkaSenderOpener so = new MyKafkaSenderOpener();

    try (KafkaSender kafkaSender = so.open()) {

      for (int i = 10; i < 20; i++) {
        String I = "" + i;
        while (I.length() < 5) I = "0" + I;
        Client client = new Client();
        client.id = "asd-" + I;
        client.surname = "Иванов " + I;
        client.name = "Иван" + I;

        kafkaSender.send(client);
      }
    }

    System.out.println("OK");
  }

}
