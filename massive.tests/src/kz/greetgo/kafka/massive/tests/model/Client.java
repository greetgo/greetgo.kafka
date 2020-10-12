package kz.greetgo.kafka.massive.tests.model;

import kz.greetgo.kafka.core.HasStrKafkaKey;

@KafkaModel
public class Client implements HasStrKafkaKey {

  public String id;
  public String surname;
  public String name;
  public String patronymic;

  public String author;

  @Override
  public String toString() {
    return "Client{" +
      "id=" + (id == null ? "null" : ('\'' + id + '\'')) +
      ", surname=" + (surname == null ? "null" : ('\'' + surname + '\'')) +
      ", name=" + (name == null ? "null" : ('\'' + name + '\'')) +
      ", patronymic=" + (patronymic == null ? "null" : ('\'' + patronymic + '\'')) +
      '}';
  }

  @Override
  public String extractStrKafkaKey() {
    return id;
  }
}
