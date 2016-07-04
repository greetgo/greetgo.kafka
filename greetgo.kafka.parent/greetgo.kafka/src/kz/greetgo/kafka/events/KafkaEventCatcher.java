package kz.greetgo.kafka.events;

public interface KafkaEventCatcher {
  boolean needCatchOf(Class<? extends KafkaEvent> eventClass);
  
  void catchEvent(KafkaEvent event);
}
