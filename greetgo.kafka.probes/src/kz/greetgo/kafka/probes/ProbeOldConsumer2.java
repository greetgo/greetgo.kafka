package kz.greetgo.kafka.probes;

import kafka.consumer.*;
import kafka.message.MessageAndMetadata;
import kafka.serializer.DefaultDecoder;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.collection.Seq;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.NoSuchElementException;
import java.util.Properties;

public class ProbeOldConsumer2 {
  public static void main(String[] args) throws IOException {

    final ConsumerConnector consumerConnector[] = new ConsumerConnector[1];

    Thread thread = new Thread(() -> {
      Properties pp = new Properties();
      pp.setProperty("auto.offset.reset", "smallest");
      pp.setProperty("group.id", "asd2");
      pp.setProperty("auto.commit.enable", "false");
      pp.setProperty("zookeeper.connect", "localhost:2181");

      final ConsumerConfig consumerConfig = new ConsumerConfig(pp);

      consumerConnector[0] = Consumer$.MODULE$.create(consumerConfig);

      final Whitelist filter = new Whitelist("pompei_gcory_starter");

      final DefaultDecoder keyDecoder = new DefaultDecoder(null);
      final DefaultDecoder valueDecoder = new DefaultDecoder(null);


      final Seq<KafkaStream<byte[], byte[]>> kafkaStream = consumerConnector[0].createMessageStreamsByFilter(
          filter, 1, keyDecoder, valueDecoder
      );
      final KafkaStream<byte[], byte[]> kafkaStreamHead = kafkaStream.head();

      final ConsumerIterator<byte[], byte[]> iterator = kafkaStreamHead.iterator();

      final StringDeserializer stringDeserializer = new StringDeserializer();


      int i = 0;

      try {

        while (true) {

          final MessageAndMetadata<byte[], byte[]> mam;
          try {
            mam = iterator.next();
          } catch (NoSuchElementException e) {
            System.out.println("--> NoSuchElementException");
            break;
          }

          final String message = stringDeserializer.deserialize("asd", mam.message());
          final String key = stringDeserializer.deserialize("asd", mam.key());

          System.out.println("i = " + ++i + ", key = " + key + ", message = " + message);
        }

      } catch (Exception e) {
        System.out.println("Hello world " + e.getClass() + " : " + e.getMessage());
        e.printStackTrace();
      }

      //consumerConnector.commitOffsets(true);
      System.out.println("consumerConnector[0].shutdown();");
      consumerConnector[0].shutdown();
    });


    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    while (true) {

      System.out.print(">");
      final String cmd = br.readLine();

      if ("stop".equals(cmd)) {
        System.out.println("thread.interrupt()");
        thread.interrupt();
        continue;
      }

      if ("exit".equals(cmd)) {
        System.out.println("Exit");
        return;
      }

      if ("shut".equals(cmd)) {
        consumerConnector[0].shutdown();
        continue;
      }

      if ("start".equals(cmd)) {
        thread.start();
        continue;
      }

      if ("status".equals(cmd)) {
        System.out.println("isAlive = " + thread.isAlive()
            + ", isDaemon = " + thread.isDaemon()
            + ", isInterrupted = " + thread.isInterrupted());
        continue;
      }

      if ("".equals(cmd)) {
        continue;
      }

      System.out.println("Unknown command " + cmd);
    }
  }
}
