package kz.greetgo.kafka.probes;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class ProducerProbe {

  private static class ToProduce extends Thread {

    private final Producer<String, String> producer;
    private final Path dataDir;
    private final AtomicBoolean working;

    public ToProduce(Producer<String, String> producer, Path dataDir, AtomicBoolean working) {
      this.producer = producer;
      this.dataDir = dataDir;
      this.working = working;

      //noinspection ResultOfMethodCallIgnored
      dataDir.toFile().mkdirs();
    }

    @Override
    public void run() {
      try {
        runEx();
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private void runEx() throws Exception {

      String topic = dataDir.toFile().getName();

      System.out.println("Started producer " + topic);

      while (working.get()) {

        List<Path> files = Files.list(dataDir).collect(Collectors.toList());

        for (Path path : files) {
          String value = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
          String key = path.toFile().getName();

          producer.send(new ProducerRecord<>(topic, key, value));

          System.out.println("sent " + key + " to " + topic);

          Files.delete(path);
        }

        Thread.sleep(100);
      }

      System.out.println("Finished producer " + topic);
    }
  }

  public static void main(String[] args) throws InterruptedException {

    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("delivery.timeout.ms", 35000);
    props.put("linger.ms", 1);
    props.put("request.timeout.ms", 30000);
    props.put("batch.size", 16384);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    Path toProduce = Paths.get("build").resolve("to_produce");
    Path topicOne = toProduce.resolve("one");
    Path topicTwo = toProduce.resolve("two");

    try (Producer<String, String> producer = new KafkaProducer<>(props)) {

      AtomicBoolean working = new AtomicBoolean(true);

      ToProduce produceOne = new ToProduce(producer, topicOne, working);
      ToProduce produceTwo = new ToProduce(producer, topicTwo, working);

      produceOne.start();
      produceTwo.start();

      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        working.set(false);
        System.out.println("Ctrl+C got : finishing...");
      }));

      produceOne.join();
      produceTwo.join();

    }

    System.out.println("Finished");
  }
}
