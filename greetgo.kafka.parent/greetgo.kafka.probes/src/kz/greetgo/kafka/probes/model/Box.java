package kz.greetgo.kafka.probes.model;

public class Box {
  public Header header;
  public Object body;

  @Override
  public String toString() {
    return "Box{" +
        "header=" + header +
        ", body=" + body +
        '}';
  }
}
