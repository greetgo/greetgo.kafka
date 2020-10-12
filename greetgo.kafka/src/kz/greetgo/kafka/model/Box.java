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
   * Kafka id
   */
  public String id;

  /**
   * Body of message
   */
  public Object body;

  @Override
  public String toString() {
    StringBuilder ret = new StringBuilder();
    ret.append("Box{");
    if (a != null) {
      ret.append("a=").append(a);
    }
    if (id != null) {
      ret.append("id=").append(id);
    }
    if (i != null) {
      ret.append("i=").append(i);
    }
    ret.append("body=").append(body);
    ret.append('}');
    return ret.toString();
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
