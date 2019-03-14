package kz.greetgo.kafka_old.events;

public interface KafkaEventCatcher {
  boolean needCatchOf(Class<? extends KafkaEvent> eventClass);
  
  void catchEvent(KafkaEvent event);
}
