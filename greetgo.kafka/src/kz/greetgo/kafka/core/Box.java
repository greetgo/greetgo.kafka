package kz.greetgo.kafka.core;

/**
 * All kafka objects has this class
 */
public class Box {
  /**
   * The head
   */
  public Head head;

  /**
   * The body
   */
  public Object body;

  public boolean isIgnore(String consumerName) {
    if (head == null) return false;
    return head.isIgnore(consumerName);
  }
}
