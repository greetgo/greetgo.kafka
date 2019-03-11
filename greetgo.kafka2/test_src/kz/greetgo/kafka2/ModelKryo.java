package kz.greetgo.kafka2;

public class ModelKryo {
  public String name;
  public int age;
  public Long wow;
  public Object hello;

  @Override
  public String toString() {
    return "ModelKryo{name='" + name + "', age=" + age + ", wow=" + wow + ", hello=" + hello + '}';
  }
}
