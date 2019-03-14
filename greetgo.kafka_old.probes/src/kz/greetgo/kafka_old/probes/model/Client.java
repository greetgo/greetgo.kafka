package kz.greetgo.kafka_old.probes.model;

public class Client {
  public String id;
  public String surname, name, patronymic;

  @Override
  public String toString() {
    return "Client{" +
        "id=" + id +
        ", name=" + surname +
        ", name= " + name +
        ", patronymic=" + patronymic +
        '}';
  }
}
