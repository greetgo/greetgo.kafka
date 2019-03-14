package kz.greetgo.kafka_old.events.e;

import kz.greetgo.kafka_old.consumer.ConsumerDefinition;
import kz.greetgo.kafka_old.events.KafkaEvent;

import java.text.SimpleDateFormat;
import java.util.List;

public abstract class ConsumerEventStartStop extends KafkaEvent {
  public final ConsumerDefinition consumerDefinition;
  public final String factCursorId;
  public final List<String> factTopicList;

  public ConsumerEventStartStop(ConsumerDefinition consumerDefinition, String factCursorId, List<String> factTopicList) {
    this.consumerDefinition = consumerDefinition;
    this.factCursorId = factCursorId;
    this.factTopicList = factTopicList;
  }

  @Override
  public String toString() {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    return sdf.format(at) + ' ' + name() + ", consumer: " + consumerDefinition.consume.name()
        + ", topics: " + factTopicList + ", cursorId: " + factCursorId;
  }
}
