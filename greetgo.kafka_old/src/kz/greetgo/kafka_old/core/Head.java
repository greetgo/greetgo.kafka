package kz.greetgo.kafka_old.core;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * Head of kafka box
 */
public class Head {
  /**
   * Indicates, when record created
   */
  public Date t;

  /**
   * Indicates, when record created, in nanoseconds (It came from {@link System#nanoTime()})
   */
  public long n;

  /**
   * Author of insertion
   */
  public String a;

  /**
   * Ignorable consumers
   */
  public Set<String> ign;

  public boolean isIgnore(String consumerName) {
    if (ign == null) return false;
    return ign.contains(consumerName);
  }
}
