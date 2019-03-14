package kz.greetgo.kafka.probes.more;

import kz.greetgo.kafka.core.HasId;

public class Client implements HasId {
  public String id;
  public String surname, name;

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String toString() {
    return "Client{" +
        "id='" + id + '\'' +
        ", name='" + surname + '\'' +
        ", name='" + name + '\'' +
        '}';
  }
}
