package kz.greetgo.kafka.consumer.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>
 * This method is notifier: it means that consumer config parameter `auto.offset.reset` = "latest"
 * </p>
 * <p>
 * If this annotation is absent, then consumer config parameter `auto.offset.reset` = "earliest"
 * </p>
 */
@Documented
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface KafkaNotifier {}
