package kz.greetgo.kafka_old.events.e;

import kz.greetgo.kafka_old.consumer.ConsumerDefinition;
import kz.greetgo.kafka_old.events.KafkaEvent;

import java.text.SimpleDateFormat;

public class ConsumerEventRegister extends KafkaEvent {
  public final ConsumerDefinition consumerDefinition;

  public ConsumerEventRegister(ConsumerDefinition consumerDefinition) {
    this.consumerDefinition = consumerDefinition;
  }

  @Override
  public String toString() {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    return sdf.format(at) + ' ' + name() + ' ' + consumerDefinition.consume.name();
  }
}
