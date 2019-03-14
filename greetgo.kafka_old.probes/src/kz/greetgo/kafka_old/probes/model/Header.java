package kz.greetgo.kafka_old.probes.model;

import java.util.Date;

public class Header {
  public Date t;
  public long n;
  public String a;

  @Override
  public String toString() {
    return "Header{" +
        "t=" + t +
        ", n=" + n +
        ", a='" + a + '\'' +
        '}';
  }
}
