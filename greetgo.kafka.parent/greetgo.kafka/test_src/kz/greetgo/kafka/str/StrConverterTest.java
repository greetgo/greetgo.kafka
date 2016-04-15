package kz.greetgo.kafka.str;

import org.junit.Test;

import java.io.StringReader;
import java.io.StringWriter;

import static org.fest.assertions.api.Assertions.assertThat;

public class StrConverterTest {

  static class Client {
    public String id;
    public String surname;
    public String name;
  }

  @Test
  public void marshall() throws Exception {
    StrConverter converter = new StrConverter();
    converter.useClass("Client123", Client.class);

    StringWriter stringWriter = new StringWriter();

    Client client = new Client();
    client.id = "asd";
    client.surname = "Привем";
    client.name = "Капитал";

    converter.marshall(client, stringWriter);

    String str = stringWriter.toString();
    System.out.println(str);
    assertThat(str).isEqualTo("Client123{id='asd',surname='Привем',name='Капитал'}");
  }

  @Test
  public void unMarshall() throws Exception {

    StringReader stringReader = new StringReader("ClientAsd{id='大家好',surname='Привем',name='Капитал'}");

    StrConverter converter = new StrConverter();
    converter.useClass("ClientAsd", Client.class);

    Object o = converter.unMarshall(stringReader);
    assertThat(o).isNotNull();
    assertThat(o).isInstanceOf(Client.class);

    Client client = (Client) o;
    assertThat(client.id).isEqualTo("大家好");
    assertThat(client.surname).isEqualTo("Привем");
    assertThat(client.name).isEqualTo("Капитал");

  }

}
