package kz.greetgo.kafka.consumer.annotations;

import kz.greetgo.kafka.consumer.InnerProducer;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Defines name of {@link InnerProducer}
 */
@Documented
@Target({ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface InnerProducerName {

  /**
   * @return producer name
   */
  String[] value();

}
