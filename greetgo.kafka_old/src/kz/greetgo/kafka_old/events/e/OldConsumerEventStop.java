package kz.greetgo.kafka_old.events.e;

import kz.greetgo.kafka_old.consumer.ConsumerDefinition;

import java.util.List;

public class OldConsumerEventStop extends ConsumerEventStartStop {
  public OldConsumerEventStop(ConsumerDefinition consumerDefinition, String factCursorId, List<String> factTopicList) {
    super(consumerDefinition, factCursorId, factTopicList);
  }
}
