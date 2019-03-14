package kz.greetgo.kafka_old.probes;

import kafka.consumer.Whitelist;

public class WhiteListProbe {
  public static void main(String[] args) {
    Whitelist wl = new Whitelist("asd|dsa");


    System.out.println("asd = " + wl.isTopicAllowed("asd", true));
    System.out.println("dsa = " + wl.isTopicAllowed("dsa", true));
    System.out.println("wow = " + wl.isTopicAllowed("wow", true));

  }
}
