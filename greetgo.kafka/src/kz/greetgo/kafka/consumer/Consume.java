package kz.greetgo.kafka.consumer;

import java.lang.annotation.*;

/**
 * Define consumer method
 */
@Documented
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Consume {
  /**
   * @return consumer name
   */
  String name();

  /**
   * @return cursor id (in Kafka's terming: group id)
   */
  String cursorId();

  /**
   * @return topic list, this consumer subscribed to
   */
  String[] topics();
}
