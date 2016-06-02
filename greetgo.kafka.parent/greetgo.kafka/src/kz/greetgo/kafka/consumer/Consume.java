package kz.greetgo.kafka.consumer;

import java.lang.annotation.*;

@Documented
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Consume {
  String cursorId();

  String[] topics();
}
