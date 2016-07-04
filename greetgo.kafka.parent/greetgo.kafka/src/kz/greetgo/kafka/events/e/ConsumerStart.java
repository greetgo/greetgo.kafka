package kz.greetgo.kafka.events.e;

import kz.greetgo.kafka.consumer.ConsumerDefinition;

import java.util.List;

public class ConsumerStart extends ConsumerStartStop {
  public ConsumerStart(ConsumerDefinition consumerDefinition, String factCursorId, List<String> factTopicList) {
    super(consumerDefinition, factCursorId, factTopicList);
  }
}
