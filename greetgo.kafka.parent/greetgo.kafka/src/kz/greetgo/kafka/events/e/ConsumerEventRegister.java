package kz.greetgo.kafka.events.e;

import kz.greetgo.kafka.consumer.ConsumerDefinition;
import kz.greetgo.kafka.events.KafkaEvent;

public class ConsumerEventRegister extends KafkaEvent {
  public final ConsumerDefinition consumerDefinition;

  public ConsumerEventRegister(ConsumerDefinition consumerDefinition) {
    this.consumerDefinition = consumerDefinition;
  }
}
