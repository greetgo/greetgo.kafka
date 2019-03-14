package kz.greetgo.kafka.consumer.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Defines errors to commit when it is happened
 */
@Documented
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface KafkaCommitOn {

  /**
   * @return list of errors to commit when it is happened
   */
  Class<? extends Throwable>[] value();

}
