package kz.greetgo.kafka.events.e;

import kz.greetgo.kafka.consumer.ConsumerDefinition;

import java.util.List;

public class ConsumerEventStart extends ConsumerEventStartStop {
  public ConsumerEventStart(ConsumerDefinition consumerDefinition, String factCursorId, List<String> factTopicList) {
    super(consumerDefinition, factCursorId, factTopicList);
  }
}
