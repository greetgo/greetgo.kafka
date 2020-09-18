package kz.greetgo.kafka.model;

import java.util.Collection;
import java.util.List;

public class Box {

  /**
   * Author of record
   */
  public String a;

  /**
   * List of ignorable consumers
   */
  public List<String> i;

  /**
   * Body of message
   */
  public Object body;

  @Override
  public String toString() {
    return "Box{a='" + a + '\'' + ", i=" + i + ", b=" + body + '}';
  }

  public static void validateBody(Object body) throws Throwable {

    if (body == null) {
      throw new NullPointerException(Box.class.getName() + ".body == null");
    }

    if (body instanceof KafkaValidator) {
      ((KafkaValidator) body).validateKafka();
      return;
    }

    if (body instanceof Collection) {
      //noinspection rawtypes
      for (Object object : ((Collection) body)) {
        validateIt(object);
      }
      return;
    }
  }

  public void validate() throws Throwable {
    validateBody(body);
  }

  private static void validateIt(Object object) throws Throwable {
    if (object instanceof KafkaValidator) {
      ((KafkaValidator) object).validateKafka();
    }
  }

}
