package kz.greetgo.kafka.events.e;

import kz.greetgo.kafka.consumer.ConsumerDefinition;

import java.util.List;

public class ConsumerEventStop extends ConsumerEventStartStop {
  public ConsumerEventStop(ConsumerDefinition consumerDefinition, String factCursorId, List<String> factTopicList) {
    super(consumerDefinition, factCursorId, factTopicList);
  }
}
