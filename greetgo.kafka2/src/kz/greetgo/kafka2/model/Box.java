package kz.greetgo.kafka2.model;

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
}
