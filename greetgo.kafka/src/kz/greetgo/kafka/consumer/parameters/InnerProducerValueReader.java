package kz.greetgo.kafka.consumer.parameters;

import com.google.common.collect.Sets;
import kz.greetgo.kafka.consumer.InnerProducer;
import kz.greetgo.kafka.consumer.InvokeSessionContext;
import kz.greetgo.kafka.consumer.ParameterValueReader;
import kz.greetgo.kafka.model.Box;
import kz.greetgo.kafka.producer.KafkaFuture;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class InnerProducerValueReader implements ParameterValueReader {

  private List<KafkaFuture> kafkaFutures = new ArrayList<>();
  private String producerName;
  private String topic;

  public InnerProducerValueReader(String producerName, String topic) {
    this.producerName = producerName;
    this.topic = topic;
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
    return
      (InnerProducer)
        model ->
          kafkaFutures.add(invokeSessionContext.getProducer(producerName)
            .sending(model)
            .toTopic(topic)
            .go());
  }

}
