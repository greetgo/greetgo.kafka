package kz.greetgo.kafka.core;

import java.util.Date;

/**
 * All kafka objects has this class
 */
public class Box {
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
   * The body
   */
  public Object body;
}
