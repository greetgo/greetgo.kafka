package kz.greetgo.kafka_old.probes;

import kafka.consumer.*;
import kafka.message.MessageAndMetadata;
import kafka.serializer.DefaultDecoder;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.collection.Iterator;
import scala.collection.Seq;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.NoSuchElementException;
import java.util.Properties;

public class ProbeOldConsumer3 {
  public static void main(String[] args) throws IOException {

    Properties pp = new Properties();
    pp.setProperty("auto.offset.reset", "smallest");
    pp.setProperty("group.id", "asd2");
    pp.setProperty("auto.commit.enable", "false");
    pp.setProperty("zookeeper.connect", "localhost:2181");

    final ConsumerConfig consumerConfig = new ConsumerConfig(pp);

    final ConsumerConnector consumerConnector = Consumer$.MODULE$.create(consumerConfig);

    final Whitelist filter = new Whitelist("pompei_gcory_small_batch_out");

    final DefaultDecoder keyDecoder = new DefaultDecoder(null);
    final DefaultDecoder valueDecoder = new DefaultDecoder(null);


    int threadCount = 2;


    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    while (true) {

      System.out.println("threadCount = " + threadCount);
      final String cmd = br.readLine();

      if ("exit".equals(cmd)) {
        System.out.println("Exit");
        return;
      }

      if (cmd.startsWith("tc ")) {
        threadCount = Integer.parseInt(cmd.substring(3).trim());
        continue;
      }

      if ("shut".equals(cmd)) {
        consumerConnector.shutdown();
        continue;
      }

      if ("start".equals(cmd)) {

        final Seq<KafkaStream<byte[], byte[]>> kafkaStream = consumerConnector.createMessageStreamsByFilter(
            filter, threadCount, keyDecoder, valueDecoder
        );

        final Iterator<KafkaStream<byte[], byte[]>> kafkaStreamIterator = kafkaStream.iterator();

        int threadIndex = 0;

        final StringDeserializer stringDeserializer = new StringDeserializer();

        while (kafkaStreamIterator.hasNext()) {
          final int ThreadIndex = threadIndex++;

          final KafkaStream<byte[], byte[]> threadKafkaStream = kafkaStreamIterator.next();

          new Thread(() -> {

            final ConsumerIterator<byte[], byte[]> iterator = threadKafkaStream.iterator();

            System.out.println("Started thread " + ThreadIndex);

            int i = 0;

            while (true) {

              final MessageAndMetadata<byte[], byte[]> mam;
              try {
                mam = iterator.next();
              } catch (NoSuchElementException e) {
                System.out.println("--> NoSuchElementException in thread index = " + ThreadIndex);
                break;
              }

              final String message = stringDeserializer.deserialize("asd", mam.message());
              final String key = stringDeserializer.deserialize("asd", mam.key());

              System.out.println("ThreadIndex = " + ThreadIndex + ", i = " + ++i
                  + ", key = " + key + ", message = " + message);

            }

            System.out.println("Exit thread " + ThreadIndex);
          }).start();

        }

        continue;
      }

      if ("".equals(cmd)) {
        continue;
      }

      System.out.println("Unknown command " + cmd);
    }
  }
}
