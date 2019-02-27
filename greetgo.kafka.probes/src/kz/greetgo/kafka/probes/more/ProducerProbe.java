package kz.greetgo.kafka.probes.more;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerProbe {

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("broker-list", "localhost:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("key.serializer", StringSerializer.class.getName());
    props.put("value.serializer", StringSerializer.class.getName());

    String pre = "lolla-A";

    long time1 = System.currentTimeMillis();

    try (KafkaProducer<String, String> kp = new KafkaProducer<>(props)) {
      for (int i = 0; i < 10; i++) {
        int I = i + 101_000_000;
        kp.send(
            new ProducerRecord<>(Params.TOPIC_NAME, pre + "-" + I, pre + "-" + I + " value"
            )).get();
        System.out.println("Sent " + i);
      }
    }

    long time2 = System.currentTimeMillis();

    System.out.println("Complete " + (time2 - time1));
  }

}
