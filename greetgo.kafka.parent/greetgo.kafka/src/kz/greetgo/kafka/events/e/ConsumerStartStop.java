package kz.greetgo.kafka.events.e;

import kz.greetgo.kafka.consumer.ConsumerDefinition;
import kz.greetgo.kafka.events.KafkaEvent;

import java.text.SimpleDateFormat;
import java.util.List;

public abstract class ConsumerStartStop extends KafkaEvent {
  public final ConsumerDefinition consumerDefinition;
  public final String factCursorId;
  public final List<String> factTopicList;

  public ConsumerStartStop(ConsumerDefinition consumerDefinition, String factCursorId, List<String> factTopicList) {
    this.consumerDefinition = consumerDefinition;
    this.factCursorId = factCursorId;
    this.factTopicList = factTopicList;
  }

  @Override
  public String toString() {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    return sdf.format(at) + ' ' + name() + " [consumerDefinition: " + consumerDefinition + ", factCursorId: "
        + factCursorId + ", factTopicList: " + factTopicList + "]";
  }
}
