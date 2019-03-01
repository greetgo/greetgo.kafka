package kz.greetgo.kafka2.probes;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerProbe {
  public static void main(String[] args) {

    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "cool");
    props.put("enable.auto.commit", "true");
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("heartbeat.interval.ms", "10000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("auto.offset.reset", "earliest");

    AtomicBoolean working = new AtomicBoolean(true);

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(Arrays.asList("one", "two"));

      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        System.out.println("Got Ctrl+C - finishing....");
        working.set(false);
        consumer.wakeup();
      }));

      while (working.get()) {

        try {
          ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

          int recCount = records.count();

          for (ConsumerRecord<String, String> record : records) {
            System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
          }

          System.out.println("End part : recCount = " + recCount);
        } catch (org.apache.kafka.common.errors.WakeupException wakeupException) {
          System.err.println("WakeupException : " + wakeupException.getMessage());
        }
      }

    }

    System.out.println("Finished");

  }
}
