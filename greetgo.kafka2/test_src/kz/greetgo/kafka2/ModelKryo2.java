package kz.greetgo.kafka2;

public class ModelKryo2 {
  public String name;
  public int age;
  public Long wow;
  public Object hello;

  @Override
  public String toString() {
    return "ModelKryo2{name='" + name + "', age=" + age + ", wow=" + wow + ", hello=" + hello + '}';
  }
}
