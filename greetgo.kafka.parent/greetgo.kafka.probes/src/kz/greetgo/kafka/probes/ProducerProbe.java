package kz.greetgo.kafka.probes;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerProbe {

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("key.serializer", StringSerializer.class.getName());
    props.put("value.serializer", StringSerializer.class.getName());

    String pre = "dsa-7";

    try (KafkaProducer<String, String> kp = new KafkaProducer<>(props)) {
      for (int i = 0; i < 5; i++) {
        int I = i + 3000000;
        kp.send(new ProducerRecord<String, String>("topic", pre + "-" + I, pre + "-" + I + " value")).get();
        System.out.println("Sent " + i);
      }
    }

    System.out.println("Complete");
  }

}
