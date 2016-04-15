package kz.greetgo.kafka.str;

import org.testng.annotations.Test;

import static org.fest.assertions.api.Assertions.assertThat;

public class FieldAcceptorTest {

  @SuppressWarnings("unused")
  static class Client {
    public Long id;

    public String surname;

    private String name;

    public String getName() {
      return name + "G";
    }

    public void setName(String name) {
      this.name = name + "Q";
    }

    public String getPatronymic() {
      return "Переход";
    }
  }

  @Test
  public void get() throws Exception {

    Client client = new Client();
    client.id = 234L;
    client.surname = "Кишилёв";
    client.setName("Владимир");

    FieldAcceptor acceptor = new FieldAcceptor(Client.class);

    assertThat(acceptor.get(client, "id")).isEqualTo(client.id);
    assertThat(acceptor.get(client, "surname")).isEqualTo(client.surname);
    assertThat(acceptor.get(client, "name")).isEqualTo(client.name + "G");
    assertThat(acceptor.get(client, "patronymic")).isEqualTo("Переход");

  }

  @Test
  public void set() throws Exception {

    Client client = new Client();
    client.id = 234L;
    client.surname = "Кишилёв";
    client.setName("Владимир");

    FieldAcceptor acceptor = new FieldAcceptor(Client.class);

    acceptor.set(client, "id", 9876L);
    assertThat(client.id).isEqualTo(9876L);

    acceptor.set(client, "surname", "Привалов");
    assertThat(client.surname).isEqualTo("Привалов");

    acceptor.set(client, "name", "Дмитрий");
    assertThat(client.name).isEqualTo("ДмитрийQ");

  }
}