package kz.greetgo.kafka_old.events;

import java.util.Date;

public abstract class KafkaEvent {
  public final Date at = new Date();

  public String name() {
    return getClass().getSimpleName();
  }
}
