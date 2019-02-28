package kz.greetgo.kafka.producer;

import kz.greetgo.kafka.core.HasId;
import kz.greetgo.kafka.core.StrConverterPreparationBased;
import kz.greetgo.strconverter.StrConverter;
import kz.greetgo.strconverter.simple.StrConverterSimple;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;

public class AbstractKafkaSenderTest {

  public static class Asd implements HasId {
    public String id;
    public String field;

    public Asd() {
    }

    public Asd(String id, String field) {
      this.id = id;
      this.field = field;
    }

    @Override
    public String getId() {
      return id;
    }
  }

  private static StrConverter testStrConverter() {
    StrConverterSimple ret = new StrConverterSimple();
    StrConverterPreparationBased.prepare(ret);
    ret.useClass(Asd.class, "Asd");
    return ret;
  }

  class MyKafkaSender extends AbstractKafkaSender {
    @Override
    protected String getBootstrapServers() {
      return "localhost:9092";
    }

    @Override
    protected StrConverter createStrConverter() {
      return testStrConverter();
    }

    @Override
    protected String author() {
      return "asd";
    }

    @Override
    protected Set<String> ignorableConsumers(String author, Object sendingObject, String key) {
      return new HashSet<>();
    }

    @Override
    protected String topic() {
      return "fasting_test";
    }
  }

  @Test
  public void sendFasting() throws Exception {

    MyKafkaSender sender = new MyKafkaSender();

    long time1 = System.currentTimeMillis();

    for (int i = 0; i < 10; i++) {
      try (KafkaSending sending = sender.open()) {
        sending.send(new Asd("asd id wow", "asd|xx"));
      }
    }

    long time2 = System.currentTimeMillis();

    System.out.println("time = " + (time2 - time1));

  }
}
