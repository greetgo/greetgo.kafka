package kz.greetgo.kafka.model;

import java.util.Collection;
import java.util.List;

public class Box {

  public String author;
  public List<String> ignorableConsumers;
  public Object body;

  @Override
  public String toString() {
    return "Box{" +
      "author='" + author + '\'' +
      ", ignorableConsumers=" + ignorableConsumers +
      ", body=" + body +
      '}';
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
