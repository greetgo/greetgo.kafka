package kz.greetgo.kafka.probes;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.File;
import java.util.Arrays;
import java.util.Properties;

public class ProbeConsumer {
  public static void main(String[] args) throws Exception {
    final Properties props = new Properties();
    props.put("bootstrap.servers", "192.168.11.185:9092");
    props.put("group.id", "asd-002-1");
    props.put("enable.auto.commit", "false");
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", StringDeserializer.class.getName());
    props.put("value.deserializer", StringDeserializer.class.getName());
    props.put("auto.offset.reset", "earliest");

    final boolean running[] = new boolean[]{true};

    Thread thread = new Thread(() -> {
      try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
        System.out.println("Consumer has been started");

        consumer.subscribe(Arrays.asList("asd-002"));
        while (running[0]) {
          ConsumerRecords<String, String> records = consumer.poll(100);
          for (ConsumerRecord<String, String> record : records) {
            String key = record.key();
            String value = record.value();
            System.out.println(key + " -> " + value);
          }

          consumer.commitSync();
        }
      }

    });

    thread.start();

    File working = new File("build/consumer_working");
    working.getParentFile().mkdirs();
    working.createNewFile();

    while (working.exists()) {
      Thread.sleep(500);
    }

    running[0] = false;
    thread.join();

    System.out.println("Consumer stopped");

  }
}
