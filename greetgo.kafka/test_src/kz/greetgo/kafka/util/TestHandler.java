package kz.greetgo.kafka.util;

import kz.greetgo.kafka.util.Handler;

public class TestHandler implements Handler {
  public int happenCount = 0;

  @Override
  public void handler() {
    happenCount++;
  }
}
