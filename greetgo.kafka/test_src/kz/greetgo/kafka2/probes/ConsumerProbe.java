package kz.greetgo.kafka2.probes;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerProbe {
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public static void main(String[] args) throws IOException {

    Properties props = new Properties();

    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("heartbeat.interval.ms", "10000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    props.put("enable.auto.commit", "false");
    props.put("auto.offset.reset", "earliest");
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "cool2");

    AtomicBoolean working = new AtomicBoolean(true);

    File workingFile = new File("build/probes/ConsumerProbe.working.txt");
    workingFile.getParentFile().mkdirs();
    workingFile.createNewFile();

    new Thread(() -> {

      while (working.get()) {

        try {
          Thread.sleep(300);
        } catch (InterruptedException e) {
          break;
        }

        if (!workingFile.exists()) {
          working.set(false);
        }

      }

    }).start();

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(Arrays.asList("one", "two"));

      while (working.get()) {

        try {
          ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

          int recCount = records.count();

          for (ConsumerRecord<String, String> record : records) {
            System.out.printf("offset = %d, topic = %s, key = %s, value = %s%n",
              record.offset(), record.topic(), record.key(), record.value());
          }

          consumer.commitSync();

//          System.out.println("End part : recCount = " + recCount);
        } catch (org.apache.kafka.common.errors.WakeupException wakeupException) {
          System.err.println("WakeupException : " + wakeupException.getMessage());
        }
      }

    }

    System.out.println("Finished");

  }
}
