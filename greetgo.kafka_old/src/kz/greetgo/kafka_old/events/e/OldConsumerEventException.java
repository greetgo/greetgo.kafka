package kz.greetgo.kafka_old.events.e;

import kz.greetgo.kafka_old.consumer.ConsumerDefinition;
import kz.greetgo.kafka_old.events.KafkaEvent;

import java.text.SimpleDateFormat;
import java.util.List;

public class OldConsumerEventException extends KafkaEvent {
  public final ConsumerDefinition consumerDefinition;
  public final String factCursorId;
  public final List<String> factTopicList;
  public final Exception exception;

  public OldConsumerEventException(ConsumerDefinition consumerDefinition,
                                   String factCursorId,
                                   List<String> factTopicList,
                                   Exception exception) {
    this.consumerDefinition = consumerDefinition;
    this.factCursorId = factCursorId;
    this.factTopicList = factTopicList;
    this.exception = exception;
  }

  @Override
  public String toString() {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    return sdf.format(at) + ' ' + name() + " [consumerDefinition: " + consumerDefinition + ", factCursorId: "
        + factCursorId + ", factTopicList: " + factTopicList + ", exception: " + exception + "]";
  }
}
