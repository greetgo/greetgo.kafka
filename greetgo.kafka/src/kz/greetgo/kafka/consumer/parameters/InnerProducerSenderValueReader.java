package kz.greetgo.kafka.consumer.parameters;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import kz.greetgo.kafka.consumer.InnerProducerSender;
import kz.greetgo.kafka.consumer.InvokeSessionContext;
import kz.greetgo.kafka.consumer.ParameterValueReader;
import kz.greetgo.kafka.model.Box;
import kz.greetgo.kafka.producer.KafkaFuture;
import kz.greetgo.kafka.producer.KafkaSending;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.Set;

public class InnerProducerSenderValueReader implements ParameterValueReader {
  private List<KafkaFuture> kafkaFutures = Lists.newArrayList();
  private String producerName;

  public InnerProducerSenderValueReader(String producerName) {
    this.producerName = producerName;
  }

  @Override
  public Set<String> getProducerNames() {
    return Sets.newHashSet(producerName);
  }

  @Override
  public List<KafkaFuture> getKafkaFutures() {
    return kafkaFutures;
  }

  @Override
  public Object read(ConsumerRecord<byte[], Box> record, InvokeSessionContext invokeSessionContext) {
    return new InnerProducerSender() {
      @Override
      public Sending sending(Object model) {
        return new Sending() {
          private KafkaSending kafkaSending = invokeSessionContext.getProducer(producerName)
              .sending(model);

          @Override
          public Sending toTopic(String topic) {
            kafkaSending.toTopic(topic);
            return this;
          }

          @Override
          public Sending toPartition(int partition) {
            kafkaSending.toPartition(partition);
            return this;
          }

          @Override
          public Sending setTimestamp(Long timestamp) {
            kafkaSending.setTimestamp(timestamp);
            return this;
          }

          @Override
          public Sending addHeader(String key, byte[] value) {
            kafkaSending.addHeader(key, value);
            return this;
          }

          @Override
          public Sending addConsumerToIgnore(String consumerName) {
            kafkaSending.addConsumerToIgnore(consumerName);
            return this;
          }

          @Override
          public Sending setAuthor(String author) {
            kafkaSending.setAuthor(author);
            return this;
          }

          @Override
          public void go() {
            kafkaFutures.add(kafkaSending.go());
          }
        };
      }


    };
  }
}