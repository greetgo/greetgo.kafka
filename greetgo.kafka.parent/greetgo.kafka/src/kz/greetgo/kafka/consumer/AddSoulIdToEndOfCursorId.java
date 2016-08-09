package kz.greetgo.kafka.consumer;

import java.lang.annotation.*;

/**
 * Adds the soul id at the end of cursor id
 */
@Documented
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface AddSoulIdToEndOfCursorId {
}
