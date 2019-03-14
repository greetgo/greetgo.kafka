package kz.greetgo.kafka_old.probes;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerProbe {

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put("bootstrap.servers", "192.168.11.185:9092");
    props.put("broker-list", "192.168.11.185:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("key.serializer", StringSerializer.class.getName());
    props.put("value.serializer", StringSerializer.class.getName());

    String pre = "dsa-7";

    long time1 = System.currentTimeMillis();

    try (KafkaProducer<String, String> kp = new KafkaProducer<>(props)) {
      for (int i = 0; i < 1_000_000; i++) {
        int I = i + 200_000_000;
        kp.send(new ProducerRecord<String, String>("asd-002", pre + "-" + I, pre + "-" + I + " value\n" +
            "dfsf dsafdsaf dafds fsafk jdfakj hgeqwfueuyq fweu qwf ewqfb euwqf uewqf ewqf fqwf\n" +
            "krjebrlke reHG IHJ YUBDS Cво адалоптывадпо туцкпш тгкцзуп ктзцп ту лдвыаплф ьжуптуцлорап в"
        )).get();
        System.out.println("Sent " + i);
      }
    }

    long time2 = System.currentTimeMillis();

    System.out.println("Complete " + (time2 - time1));
  }

}
