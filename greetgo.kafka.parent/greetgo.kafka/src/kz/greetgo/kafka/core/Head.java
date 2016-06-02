package kz.greetgo.kafka.core;

import java.util.Date;

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
   * Comma-separated list of names of ignorable consumers
   */
  public String ign;
}
