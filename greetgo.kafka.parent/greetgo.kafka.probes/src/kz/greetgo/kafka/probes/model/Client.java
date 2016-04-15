package kz.greetgo.kafka.probes.model;

public class Client {
  public String id;
  public String surname, name, patronymic;

  @Override
  public String toString() {
    return "Client{" +
        "id=" + id +
        ", surname=" + surname +
        ", name= " + name +
        ", patronymic=" + patronymic +
        '}';
  }
}
