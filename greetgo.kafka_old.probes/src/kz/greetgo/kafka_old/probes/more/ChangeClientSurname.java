package kz.greetgo.kafka_old.probes.more;

public class ChangeClientSurname extends ChangeClient {
  public String surname;

  public ChangeClientSurname() {
  }

  public ChangeClientSurname(String id, String surname) {
    this.id = id;
    this.surname = surname;
  }

  @Override
  public String toString() {
    return "ChangeClientSurname{" +
        "id='" + id + '\'' +
        ", name='" + surname + '\'' +
        '}';
  }
}
