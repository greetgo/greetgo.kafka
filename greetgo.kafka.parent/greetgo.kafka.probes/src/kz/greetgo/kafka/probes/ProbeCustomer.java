package kz.greetgo.kafka.probes;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.CompactWriter;
import kz.greetgo.class_scanner.ClassScanner;
import kz.greetgo.class_scanner.ClassScannerDef;
import kz.greetgo.kafka.probes.model.Box;
import kz.greetgo.kafka.probes.model.Client;
import kz.greetgo.kafka.probes.model.Header;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class ProbeCustomer {

  public static void main(String[] args) throws Exception {
    long timeMillis = System.currentTimeMillis();
    long nanoTime = System.nanoTime();

    System.out.println("nanoTime = " + nanoTime);
    System.out.println("timeMillis = " + timeMillis);
    System.out.println("nanoTime/1_000_000 = " + nanoTime / 1000000L);

    Client client1 = new Client();
    client1.id = "asd-547545674";
    client1.surname = "dsa-123421421";
    client1.name = "asd-asd-67864545434";

    Client client2 = new Client();
    client2.id = "asd-23";
    client2.surname = "34n5k34n";
    client2.name = "втыалофвыталот";

    List<Client> clients = new ArrayList<Client>();
    clients.add(client1);
    clients.add(client2);

    Box box = new Box();
    box.body = clients;
    box.header = new Header();
    box.header.t = new Date();
    box.header.n = System.nanoTime();
    box.header.a = "asd 大家好 <好>Помидор</好>";

    XStream xStream = new XStream();

    ClassScanner scanner = new ClassScannerDef();
    for (Class<?> aClass : scanner.scanPackage("kz.greetgo.kafka.probes.model")) {
      xStream.alias(aClass.getSimpleName(), aClass);
    }

    StringWriter stringWriter = new StringWriter();
    //xStream.marshal(box, new PrettyPrintWriter(stringWriter));
    xStream.marshal(box, new CompactWriter(stringWriter));

    String xml = stringWriter.toString();

    System.out.println();
    System.out.println(xml);

    Object o = xStream.fromXML(xml);
    System.out.println();
    System.out.println(o);
  }

}
