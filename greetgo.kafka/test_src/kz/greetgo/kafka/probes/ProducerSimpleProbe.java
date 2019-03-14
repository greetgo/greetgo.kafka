package kz.greetgo.kafka.probes;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerSimpleProbe {

  public static void main(String[] args) {

    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("buffer.memory", 33554432);
    props.put("delivery.timeout.ms", 35000);
    props.put("linger.ms", 1);
    props.put("request.timeout.ms", 30000);
    props.put("batch.size", 16384);

    StringSerializer serializer = new StringSerializer();

    try (Producer<String, String> producer = new KafkaProducer<>(props, serializer, serializer)) {

      producer.send(new ProducerRecord<>("one", "asd", "asd1"));
    }

    System.out.println("Finished");

  }
}
