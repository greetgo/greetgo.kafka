package kz.greetgo.kafka.probes.more;

public class ChangeClientName extends ChangeClient {
  public String name;

  public ChangeClientName() {
  }

  public ChangeClientName(String id, String name) {
    this.id = id;
    this.name = name;
  }

  @Override
  public String toString() {
    return "ChangeClientName{" +
        "id='" + id + '\'' +
        ", name='" + name + '\'' +
        '}';
  }

}
