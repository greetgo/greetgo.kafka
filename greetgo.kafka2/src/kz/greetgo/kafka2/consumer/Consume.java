package kz.greetgo.kafka2.consumer;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Define consumer method
 */
@Documented
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Consume {

  /**
   * @return cursor id (in Kafka's terming: group id)
   */
  String cursorId();

  /**
   * @return topic list, this consumer subscribed to
   */
  String[] topics();

}
