package kz.greetgo.kafka.events.e;

import kz.greetgo.kafka.consumer.ConsumerDefinition;

import java.util.List;

public class NewConsumerEventStop extends ConsumerEventStartStop {
  public NewConsumerEventStop(ConsumerDefinition consumerDefinition, String factCursorId, List<String> factTopicList) {
    super(consumerDefinition, factCursorId, factTopicList);
  }
}
