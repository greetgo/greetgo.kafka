package kz.greetgo.kafka_old.str;

import kz.greetgo.strconverter.StrConverter;
import kz.greetgo.util.RND;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Date;

import static org.fest.assertions.api.Assertions.assertThat;

public class StrConverterTest {

  private StrConverter converter;

  @BeforeMethod
  public void setup() {
    converter = new StrConverterXml();
  }

  static class Client {
    public String id;
    public String surname;
    public String name;
    public Date birthDate;
  }

  @Test(enabled = false)
  public void toStr_fromStr() {
    converter.useClass(Client.class, "Client123");

    Client client = new Client();
    client.id = RND.str(10);
    client.surname = RND.str(10);
    client.name = RND.str(10);
    client.birthDate = RND.dateDays(-10000, 0);

    //
    //
    String str = converter.toStr(client);
    Client actual = converter.fromStr(str);
    //
    //

    assertThat(actual).isNotNull();
    assertThat(actual.id).isEqualTo(client.id);
    assertThat(actual.surname).isEqualTo(client.surname);
    assertThat(actual.name).isEqualTo(client.name);
    assertThat(actual.birthDate).isEqualTo(client.birthDate);


  }


}
