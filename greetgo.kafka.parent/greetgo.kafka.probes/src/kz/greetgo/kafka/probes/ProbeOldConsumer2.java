package kz.greetgo.kafka.probes;

import kafka.consumer.*;
import kafka.message.MessageAndMetadata;
import kafka.serializer.DefaultDecoder;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.collection.Seq;

import java.util.Properties;

public class ProbeOldConsumer2 {
  public static void main(String[] args) {
    Properties pp = new Properties();
    pp.setProperty("auto.offset.reset", "smallest");
    pp.setProperty("group.id", "asd1");
    pp.setProperty("auto.commit.enable", "false");
    pp.setProperty("zookeeper.connect", "localhost:2181");


    final ConsumerConfig consumerConfig = new ConsumerConfig(pp);

    final ConsumerConnector consumerConnector = Consumer$.MODULE$.create(consumerConfig);

    final Whitelist filter = new Whitelist("pompei_gcory_dt_exe_in");

    final DefaultDecoder keyDecoder = new DefaultDecoder(null);
    final DefaultDecoder valueDecoder = new DefaultDecoder(null);


    final Seq<KafkaStream<byte[], byte[]>> kafkaStream = consumerConnector.createMessageStreamsByFilter(
        filter, 1, keyDecoder, valueDecoder
    );
    final KafkaStream<byte[], byte[]> kafkaStreamHead = kafkaStream.head();

    final ConsumerIterator<byte[], byte[]> iterator = kafkaStreamHead.iterator();

    final StringDeserializer stringDeserializer = new StringDeserializer();

    int i = 0;

    while (iterator.hasNext()) {

      final MessageAndMetadata<byte[], byte[]> mam = iterator.next();

      final String message = stringDeserializer.deserialize("asd", mam.message());
      final String key = stringDeserializer.deserialize("asd", mam.key());

      System.out.println("i = " + ++i + ", key = " + key + ", message = " + message);

      if (i > 10) break;
    }

    //consumerConnector.commitOffsets(true);
    consumerConnector.shutdown();
  }
}
