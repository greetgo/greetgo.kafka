package kz.greetgo.kafka_old.probes.more;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.File;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerProbe {
  public static void main(String[] args) throws Exception {
    final Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("enable.auto.commit", "false");
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", StringDeserializer.class.getName());
    props.put("value.deserializer", StringDeserializer.class.getName());
    props.put("auto.offset.reset", "earliest");

    props.put("group.id", "cursor-A");

    final boolean running[] = new boolean[]{true};

    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
          System.out.println("More Consumer has been started");

          consumer.subscribe(Arrays.asList(Params.TOPIC_NAME));
          while (running[0]) {
            ConsumerRecords<String, String> records = consumer.poll(200);

            for (ConsumerRecord<String, String> record : records) {
              String key = record.key();
              String value = record.value();
              System.out.println("~~~ " + key + " -> " + value);
            }

            consumer.commitSync();
          }
        }

      }
    });

    thread.start();

    File working = new File("build/more_consumer_working");
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
